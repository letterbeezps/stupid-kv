use std::cmp::Ordering;

use bytes::Bytes;
use smallvec::SmallVec;

use crate::versions::Version;

pub(crate) enum IndexOrUpdate<'a> {
	/// 忽视
	Ignore,
	/// 在指定位置插入数据
	Index(usize),
	/// 更新数据
	Update(&'a mut Version),
}

pub struct Versions {
    inner: SmallVec<[Version; 4]>,
}

impl From<Version> for Versions {
    fn from(version: Version) -> Self {
        let mut inner = SmallVec::new();
        inner.push(version);
        Versions { 
            inner 
        }
    }
}

impl Versions {
    #[inline]
    pub(crate) fn new() -> Self {
        Versions { inner: SmallVec::new() }
    }

    #[inline]
    pub(crate) fn push(&mut self, value: Version) {
        if let Some(last) = self.inner.last_mut() {
            match value.version.cmp(&last.version) {
                Ordering::Greater => {
                    // 新版本大于最后一个版本，且值不同，直接插入
                    if value.value != last.value {
                        self.inner.push(value);
                    }
					// 新版本大于最后一个版本，且值相同，不作修改，直接返回
                    return;
                }
                Ordering::Equal => {
                    // 新版本等于最后一个版本，且值不同，更新
                    if value.value != last.value {
                        last.value = value.value;
                    }
                    return;
                }
                Ordering::Less => {
                    // 新版本小于最后一个版本，特殊情况，详见fetch_index_or_update
                }
            }
        } else {
            // 空的版本列表，直接插入
            if value.value.is_some() {
                self.inner.push(value);
            }
            return;
        }

        // 非空版本列表，插入到指定位置
        match self.fetch_index_or_update(&value) {
            IndexOrUpdate::Ignore => {
                // 忽略
            }
            IndexOrUpdate::Index(idx) => {
                self.inner.insert(idx, value);
            }
            IndexOrUpdate::Update(existing) => {
                existing.value = value.value;
            }
        }
    }

    #[inline]
    pub(crate) fn fetch_index_or_update(&mut self, value: &Version) -> IndexOrUpdate<'_> {
        let idx = self.find_index_lte_version(value.version);
        if idx == 0 {
            if value.value.is_none() {
                return IndexOrUpdate::Ignore;
            }
            return IndexOrUpdate::Index(idx);
        }
        if let Some(existing) = self.inner.get_mut(idx - 1) {
            if existing.version == value.version {
                if existing.value == value.value {
                    return IndexOrUpdate::Ignore;
                }
                return IndexOrUpdate::Update(existing);
            }
            if existing.value == value.value {
                return IndexOrUpdate::Ignore;
            }
            return IndexOrUpdate::Index(idx);
        }

        IndexOrUpdate::Index(idx)
    }

	/// 按索引区间删除内部版本；容量远大于长度时触发 `shrink_to_fit`。
	/// 阈值 `len.max(4) * 2` 保留 SmallVec 内联的 4 个 slot，避免 inline/heap 抖动。
	#[inline]
	pub fn drain<R>(&mut self, range: R)
	where
		R: std::ops::RangeBounds<usize>
	{
		self.inner.drain(range);
		if self.inner.capacity() > self.inner.len().max(4).saturating_mul(2) {
			self.inner.shrink_to_fit();
		}
	}

    /// 返回从左到右，返回idx，[0, idx) 是小于version的版本号 [idx, self.inner.len()) 是大于等于version的版本号, idx 为0表示没有找到小于version的版本号
    /// 当返回的长度等于self.inner.len()时，version大于所有版本号，所以就指向len()也就是 idx + 1
    #[inline]
    pub(crate) fn find_index_lt_version(&self, version: u64) -> usize {
        if let Some(last) = self.inner.last() {
            if version > last.version {
                return self.inner.len();
            }
        }
        if self.inner.len() <= 4 {
            self.inner
            .iter()
            .rposition(|v| v.version < version)
            .map_or(0, |i| i + 1)
        } else {
            self.inner
            .partition_point(|v| v.version < version)
        }
    }

    /// 返回从左到右，返回idx，[0, idx) 是小于等于version的版本号 [idx, self.inner.len()) 是大于version的版本号, idx 为0表示没有找到小于等于version的版本号
    /// 当返回的长度等于self.inner.len()时，version大于所有版本号，所以就指向len()也就是 idx + 1
    #[inline]
    pub(crate) fn find_index_lte_version(&self, version: u64) -> usize {
        if let Some(last) = self.inner.last() {
            if version >= last.version {
                return self.inner.len();
            }
        }
        if self.inner.len() <= 4 {
            self.inner
            .iter()
            .rposition(|v| v.version <= version)
            .map_or(0, |i| i + 1)
        } else {
            self.inner
            .partition_point(|v| v.version <= version)
        }
    }

    /// 获取指定版本号的值
    /// 先确认当前是否有小于等于version的版本号，如果有，返回该其中最大的版本号的值，否则返回None
    #[inline]
    pub(crate) fn fetch_version(&self, version: u64) -> Option<Bytes> {
        let idx = self.find_index_lte_version(version);
        if idx > 0 { 
            self.inner
            .get(idx - 1)
            .and_then(|v| v.value.clone())
        } else {
            None
        }
    }

    #[inline]
    pub(crate) fn exists_version(&self, version: u64) -> bool {
        let idx = self.find_index_lte_version(version);
        if idx > 0 {
            self.inner
            .get(idx - 1)
            .is_some_and(|v| v.value.is_some())
        } else {
            false
        }
    }

	#[inline]
	/// 导出完整版本链为可序列化的 `Vec<(u64, Option<Bytes>)>`。
	///
	/// 仅快照序列化路径使用；在线读 / 写路径调用的是 `fetch_version` / `exists_version` / `push`。
	///
	/// 导出语义（与 load 时 `Versions::push` 对称）：
	/// - 版本按 `version` 升序排列，与 `push` 后 SmallVec 内部顺序严格一致；
	/// - 返回值是独立克隆的 Vec，**不依赖调用方持有的 RwLock 读锁**；
	///   快照线程可以在释放读锁后再做 bincode encode，避免长时间占锁阻塞写路径。
	pub(crate) fn all_versions(&self) -> Vec<(u64, Option<Bytes>)> {
		self.inner
		.iter()
		.map(|v| (v.version, v.value.clone()))
		.collect()
	}

	/// 就地压缩版本链：丢弃所有活跃事务不可见的旧版本，返回压缩后的版本数。
	///
	/// 入参 `version` 是 GC 水位线 `cleanup_ts`（详见 `Inner::compute_cleanup_ts`），
	/// 保证任何活跃事务的快照都 `> version`。
	///
	/// 保留规则（`lte = find_index_lte_version(version)`，`visible = lte - 1`）：
	/// - `lte == 0`：所有版本都 `> version`，无可回收；
	/// - `versions[visible]` 是 tombstone：整条 `..lte` 全丢（活跃事务看到的就是"已删除"，
	///   等价于 key 不存在；版本链为空后调用方摘除 datastore entry）；
	/// - 否则：保留 `visible` 这条"水位线下最新可见值"，丢弃 `..visible`。
	#[inline]
	pub(crate) fn gc_older_versions(&mut self, version: u64) -> usize {
		let lte = self.find_index_lte_version(version);
		if lte == 0 {
			return self.inner.len();
		}

		let visible = lte - 1;
		if self.inner[visible].value.is_none() {
			// tombstone：连它一起丢，版本链为空后调用方会摘除 entry
			self.drain(..lte);
		} else {
			// 保留 visible，丢弃更旧的历史
			self.drain(..visible);
		}

		self.inner.len()
	}
}

#[cfg(test)]
mod test {
    use super::*;
    use bytes::Bytes;

    fn make_version(version: u64, value: Option<&str>) -> Version {
        Version {
            version,
            value: value.map(|v| Bytes::from(v.to_string())),
        }
    }

    fn make_versions(versions: Vec<(u64, Option<&str>)>) -> Versions {
        let mut v = Versions::new();
        for (version, value) in versions {
            v.push(make_version(version, value));
        }
        v
    }

	// ==================== Tests for find_index_lt_version ====================

    #[test]
    fn test_find_index_lt_version_empty() {
        let versions = make_versions(vec![]);
        // Empty versions, should return 0
        assert_eq!(versions.find_index_lt_version(10), 0);
    }

    #[test]
    fn test_find_index_lt_version_single_element() {
        let versions = make_versions(vec![(5, Some("val"))]);

        // version < only version, returns 0
        assert_eq!(versions.find_index_lt_version(3), 0);

        // version == only version, returns 0
        assert_eq!(versions.find_index_lt_version(5), 0);

        // version > only version, returns len() = 1
        assert_eq!(versions.find_index_lt_version(10), 1);
    }

    #[test]
    fn test_find_index_lt_version_multiple_elements() {
        let versions = make_versions(vec![
            (1, Some("a")),
            (3, Some("b")),
            (5, Some("c")),
            (7, Some("d")),
        ]);

        // version less than all
        assert_eq!(versions.find_index_lt_version(0), 0);

        // version between elements
        assert_eq!(versions.find_index_lt_version(2), 1); // [0,1) < 2, versions [1]; [1,4) >= 2, versions [3,5,7]
        assert_eq!(versions.find_index_lt_version(4), 2); // [0,2) < 4, versions [1,3]; [2,4) >= 4, versions [5,7]
        assert_eq!(versions.find_index_lt_version(6), 3); // [0,3) < 6, versions [1,3,5]; [3,4) >= 6, versions [7]

        // version equal to element - element is NOT less than itself, so partition point is at the element
        assert_eq!(versions.find_index_lt_version(3), 1); // [0,1) < 3, versions [1]; [1,4) >= 3, versions [3,5,7]
        assert_eq!(versions.find_index_lt_version(5), 2); // [0,2) < 5, versions [1,3]; [2,4) >= 5, versions [5,7]

        // version greater than all
        assert_eq!(versions.find_index_lt_version(10), 4);
    }

    #[test]
    fn test_find_index_lt_version_duplicate_versions() {
        let versions = make_versions(vec![
            (5, Some("a")),
            (5, Some("b")), // Same version, should update not insert
            (10, Some("c")),
        ]);

        // After push, versions are: [(5, "b"), (10, "c")]
        // For version 5: 5 is NOT < 5, so idx = 0
        assert_eq!(versions.find_index_lt_version(5), 0);
        // For version 8: 5 < 8 but 10 >= 8, so idx = 1
        assert_eq!(versions.find_index_lt_version(8), 1);
        // For version 10: 5 < 10 but 10 is NOT < 10, so idx = 1
        assert_eq!(versions.find_index_lt_version(10), 1);
        // For version 12: both 5 and 10 < 12, so idx = 2
        assert_eq!(versions.find_index_lt_version(12), 2);
    }

	// ==================== Tests for find_index_lte_version ====================

	#[test]
	fn test_find_index_lte_version_empty() {
		let versions = Versions::new();
		assert_eq!(versions.find_index_lte_version(0), 0);
		assert_eq!(versions.find_index_lte_version(1), 0);
		assert_eq!(versions.find_index_lte_version(100), 0);
	}

	#[test]
	fn test_find_index_lte_version_single_version() {
		let versions = make_versions(vec![(10, Some("value"))]);
		// Query before the version
		assert_eq!(versions.find_index_lte_version(5), 0);
		assert_eq!(versions.find_index_lte_version(9), 0);
		// Query at the version
		assert_eq!(versions.find_index_lte_version(10), 1);
		// Query after the version
		assert_eq!(versions.find_index_lte_version(11), 1);
		assert_eq!(versions.find_index_lte_version(100), 1);
	}

	#[test]
	fn test_find_index_lte_version_multiple_versions() {
		// Create a small list (≤32 elements) to trigger linear search
		let versions = make_versions(vec![
			(10, Some("v1")),
			(20, Some("v2")),
			(30, Some("v3")),
			(40, Some("v4")),
			(50, Some("v5")),
		]);
		// Query before the first version
		assert_eq!(versions.find_index_lte_version(0), 0);
		assert_eq!(versions.find_index_lte_version(5), 0);
		// Query at the first version
		assert_eq!(versions.find_index_lte_version(10), 1);
		// Query after the first version
		assert_eq!(versions.find_index_lte_version(15), 1);
		// Query at the second version
		assert_eq!(versions.find_index_lte_version(20), 2);
		// Query after the second version
		assert_eq!(versions.find_index_lte_version(25), 2);
		// Query at the third version
		assert_eq!(versions.find_index_lte_version(30), 3);
		// Query after the third version
		assert_eq!(versions.find_index_lte_version(35), 3);
		// Query at the fourth version
		assert_eq!(versions.find_index_lte_version(40), 4);
		// Query after the fourth version
		assert_eq!(versions.find_index_lte_version(45), 4);
		// Query at the fifth version
		assert_eq!(versions.find_index_lte_version(50), 5);
		// Query after the fifth version
		assert_eq!(versions.find_index_lte_version(51), 5);
		assert_eq!(versions.find_index_lte_version(100), 5);
	}

	#[test]
	fn test_find_index_lte_version_with_deletes() {
		let versions = make_versions(vec![
			(10, Some("v1")),
			(20, None), // Delete
			(30, Some("v3")),
			(40, None), // Delete
		]);
		// Query at the first version
		assert_eq!(versions.find_index_lte_version(10), 1);
		// Query after the first version
		assert_eq!(versions.find_index_lte_version(15), 1);
		// Query at the second version
		assert_eq!(versions.find_index_lte_version(20), 2);
		// Query after the second version
		assert_eq!(versions.find_index_lte_version(25), 2);
		// Query at the third version
		assert_eq!(versions.find_index_lte_version(30), 3);
		// Query after the third version
		assert_eq!(versions.find_index_lte_version(35), 3);
		// Query at the fourth version
		assert_eq!(versions.find_index_lte_version(40), 4);
		// Query after the fourth version
		assert_eq!(versions.find_index_lte_version(50), 4);
	}

	#[test]
	fn test_find_index_lt_vs_lte_difference() {
		// This test demonstrates the key difference between < and <=
		let versions = make_versions(vec![
			(10, Some("v1")),
			(20, Some("v2")),
			(30, Some("v3")),
			(40, Some("v4")),
			(50, Some("v5")),
		]);
		// Query at the first version
		assert_eq!(versions.find_index_lt_version(10), 0);
		assert_eq!(versions.find_index_lte_version(10), 1);
		// Query after the first version
		assert_eq!(versions.find_index_lt_version(15), 1);
		assert_eq!(versions.find_index_lte_version(15), 1);
		// Query at the second version
		assert_eq!(versions.find_index_lt_version(20), 1);
		assert_eq!(versions.find_index_lte_version(20), 2);
		// Query at the third version
		assert_eq!(versions.find_index_lt_version(30), 2);
		assert_eq!(versions.find_index_lte_version(30), 3);
		// Query after the third version
		assert_eq!(versions.find_index_lt_version(35), 3);
		assert_eq!(versions.find_index_lte_version(35), 3);
	}

	// ==================== Tests for push ====================

	#[test]
	fn test_push_to_empty_list() {
		let mut versions = Versions::new();
		// Push a value to empty list
		versions.push(make_version(10, Some("v1")));
		assert_eq!(versions.inner.len(), 1);
		assert_eq!(versions.fetch_version(10), Some(Bytes::from("v1".to_string())));
	}

	#[test]
	fn test_push_delete_to_empty_list() {
		let mut versions = Versions::new();
		// Push a delete (None) to empty list - should not add
		versions.push(make_version(10, None));
		assert_eq!(versions.inner.len(), 0);
	}

	#[test]
	fn test_push_in_order() {
		let mut versions = Versions::new();
		// Push versions in increasing order
		versions.push(make_version(10, Some("v1")));
		versions.push(make_version(20, Some("v2")));
		versions.push(make_version(30, Some("v3")));
		assert_eq!(versions.inner.len(), 3);
		assert_eq!(versions.fetch_version(10), Some(Bytes::from("v1".to_string())));
		assert_eq!(versions.fetch_version(20), Some(Bytes::from("v2".to_string())));
		assert_eq!(versions.fetch_version(30), Some(Bytes::from("v3".to_string())));
	}

	#[test]
	fn test_push_duplicate_values() {
		let mut versions = Versions::new();
		// Push first version
		versions.push(make_version(10, Some("v1")));
		assert_eq!(versions.inner.len(), 1);
		// Push same value at newer version - should be skipped
		versions.push(make_version(20, Some("v1")));
		assert_eq!(versions.inner.len(), 1);
		// Push different value - should be added
		versions.push(make_version(30, Some("v2")));
		assert_eq!(versions.inner.len(), 2);
		// Push same value again - should be skipped
		versions.push(make_version(40, Some("v2")));
		assert_eq!(versions.inner.len(), 2);
	}

	#[test]
	fn test_push_out_of_order() {
		let mut versions = Versions::new();
		// Push versions out of order
		versions.push(make_version(30, Some("v3")));
		versions.push(make_version(10, Some("v1")));
		versions.push(make_version(20, Some("v2")));
		// Should be sorted correctly
		assert_eq!(versions.inner.len(), 3);
		assert_eq!(versions.inner[0].version, 10);
		assert_eq!(versions.inner[1].version, 20);
		assert_eq!(versions.inner[2].version, 30);
	}

	#[test]
	fn test_push_with_deletes() {
		let mut versions = Versions::new();
		// Push value, then delete, then value again
		versions.push(make_version(10, Some("v1")));
		assert_eq!(versions.inner.len(), 1);
		// Push delete
		versions.push(make_version(20, None));
		assert_eq!(versions.inner.len(), 2);
		assert!(!versions.exists_version(20));
		// Push new value
		versions.push(make_version(30, Some("v3")));
		assert_eq!(versions.inner.len(), 3);
		assert!(versions.exists_version(30));
	}

	#[test]
	fn test_push_same_version_different_value() {
		let mut versions = Versions::new();
		// Push a version
		versions.push(make_version(10, Some("v1")));
		assert_eq!(versions.inner.len(), 1);
		// Push same version with different value - should update/replace
		versions.push(make_version(10, Some("v2")));
		assert_eq!(versions.inner.len(), 1);
		// The new value should have replaced the old one
		assert_eq!(versions.fetch_version(10), Some(Bytes::from("v2".to_string())));
	}

	#[test]
	fn test_push_same_version_same_value() {
		let mut versions = Versions::new();
		// Push a version
		versions.push(make_version(10, Some("v1")));
		assert_eq!(versions.inner.len(), 1);
		// Push same version with same value - should still update (no-op)
		versions.push(make_version(10, Some("v1")));
		assert_eq!(versions.inner.len(), 1);
		assert_eq!(versions.fetch_version(10), Some(Bytes::from("v1".to_string())));
	}

	// ==================== Fast Path Tests ====================

	#[test]
	fn test_push_fast_path_append_different_value() {
		let mut versions = Versions::new();
		versions.push(make_version(10, Some("v1")));
		versions.push(make_version(20, Some("v2")));
		// Fast path: append with different value
		versions.push(make_version(30, Some("v3")));
		assert_eq!(versions.inner.len(), 3);
		assert_eq!(versions.inner[2].version, 30);
		assert_eq!(versions.fetch_version(30), Some(Bytes::from("v3".to_string())));
	}

	#[test]
	fn test_push_fast_path_append_same_value() {
		let mut versions = Versions::new();
		versions.push(make_version(10, Some("v1")));
		versions.push(make_version(20, Some("v2")));
		// Fast path: append with same value as last - should be ignored
		versions.push(make_version(30, Some("v2")));
		assert_eq!(versions.inner.len(), 2);
		assert_eq!(versions.fetch_version(30), Some(Bytes::from("v2".to_string())));
	}

	#[test]
	fn test_push_fast_path_update_last_different_value() {
		let mut versions = Versions::new();
		versions.push(make_version(10, Some("v1")));
		versions.push(make_version(20, Some("v2")));
		// Fast path: update last version with different value
		versions.push(make_version(20, Some("v2_updated")));
		assert_eq!(versions.inner.len(), 2);
		assert_eq!(versions.fetch_version(20), Some(Bytes::from("v2_updated".to_string())));
	}

	#[test]
	fn test_push_fast_path_update_last_same_value() {
		let mut versions = Versions::new();
		versions.push(make_version(10, Some("v1")));
		versions.push(make_version(20, Some("v2")));
		// Fast path: update last version with same value - no-op
		versions.push(make_version(20, Some("v2")));
		assert_eq!(versions.inner.len(), 2);
		assert_eq!(versions.fetch_version(20), Some(Bytes::from("v2".to_string())));
	}

	#[test]
	fn test_push_fast_path_multiple_updates_to_last() {
		let mut versions = Versions::new();
		versions.push(make_version(10, Some("v1")));
		// Multiple sequential updates to the same version
		versions.push(make_version(10, Some("v2")));
		versions.push(make_version(10, Some("v3")));
		versions.push(make_version(10, Some("v4")));
		assert_eq!(versions.inner.len(), 1);
		assert_eq!(versions.fetch_version(10), Some(Bytes::from("v4".to_string())));
	}

	#[test]
	fn test_push_fast_path_alternating_append_update() {
		let mut versions = Versions::new();
		// Append version 10
		versions.push(make_version(10, Some("v1")));
		// Append version 20
		versions.push(make_version(20, Some("v2")));
		// Update version 20
		versions.push(make_version(20, Some("v2_updated")));
		// Append version 30
		versions.push(make_version(30, Some("v3")));
		// Update version 30
		versions.push(make_version(30, Some("v3_updated")));

		assert_eq!(versions.inner.len(), 3);
		assert_eq!(versions.fetch_version(10), Some(Bytes::from("v1".to_string())));
		assert_eq!(versions.fetch_version(20), Some(Bytes::from("v2_updated".to_string())));
		assert_eq!(versions.fetch_version(30), Some(Bytes::from("v3_updated".to_string())));
	}

	#[test]
	fn test_push_slow_path_insert_middle() {
		let mut versions = Versions::new();
		versions.push(make_version(10, Some("v1")));
		versions.push(make_version(30, Some("v3")));
		// Slow path: insert in the middle (version < last.version)
		versions.push(make_version(20, Some("v2")));

		assert_eq!(versions.inner.len(), 3);
		assert_eq!(versions.inner[0].version, 10);
		assert_eq!(versions.inner[1].version, 20);
		assert_eq!(versions.inner[2].version, 30);
	}

	#[test]
	fn test_push_slow_path_insert_beginning() {
		let mut versions = Versions::new();
		versions.push(make_version(20, Some("v2")));
		versions.push(make_version(30, Some("v3")));
		// Slow path: insert at the beginning
		versions.push(make_version(10, Some("v1")));

		assert_eq!(versions.inner.len(), 3);
		assert_eq!(versions.inner[0].version, 10);
		assert_eq!(versions.inner[1].version, 20);
		assert_eq!(versions.inner[2].version, 30);
	}

	#[test]
	fn test_push_slow_path_update_middle() {
		let mut versions = Versions::new();
		versions.push(make_version(10, Some("v1")));
		versions.push(make_version(20, Some("v2")));
		versions.push(make_version(30, Some("v3")));
		// Slow path: update a middle version
		versions.push(make_version(20, Some("v2_updated")));

		assert_eq!(versions.inner.len(), 3);
		assert_eq!(versions.fetch_version(20), Some(Bytes::from("v2_updated".to_string())));
	}

	#[test]
	fn test_push_with_delete_at_end() {
		let mut versions = Versions::new();
		versions.push(make_version(10, Some("v1")));
		versions.push(make_version(20, Some("v2")));
		// Fast path: append delete
		versions.push(make_version(30, None));

		assert_eq!(versions.inner.len(), 3);
		assert!(!versions.exists_version(30));
		assert_eq!(versions.fetch_version(30), None);
	}

	#[test]
	fn test_push_delete_then_value_same_version() {
		let mut versions = Versions::new();
		versions.push(make_version(10, Some("v1")));
		// Push delete
		versions.push(make_version(20, None));
		assert!(!versions.exists_version(20));
		// Update same version with a value
		versions.push(make_version(20, Some("v2")));
		assert_eq!(versions.inner.len(), 2);
		assert!(versions.exists_version(20));
		assert_eq!(versions.fetch_version(20), Some(Bytes::from("v2".to_string())));
	}

	#[test]
	fn test_push_value_then_delete_same_version() {
		let mut versions = Versions::new();
		versions.push(make_version(10, Some("v1")));
		versions.push(make_version(20, Some("v2")));
		// Update last version to delete
		versions.push(make_version(20, None));

		assert_eq!(versions.inner.len(), 2);
		assert!(!versions.exists_version(20));
		assert_eq!(versions.fetch_version(20), None);
	}

	#[test]
	fn test_push_consecutive_deletes() {
		let mut versions = Versions::new();
		versions.push(make_version(10, Some("v1")));
		// Push delete at version 20
		versions.push(make_version(20, None));
		// Push another delete at version 30 (different from last value which is None)
		versions.push(make_version(30, None));

		// Should only have 2 entries - version 20 and 30 deletes should be separate
		assert_eq!(versions.inner.len(), 2);
		assert_eq!(versions.inner[0].version, 10);
		assert_eq!(versions.inner[1].version, 20);
		assert!(!versions.exists_version(20));
		assert!(!versions.exists_version(30));
	}

	#[test]
	fn test_push_stress_many_appends() {
		let mut versions = Versions::new();
		// Push many versions in order (all fast path appends)
		for i in 0..100 {
			let value = format!("v{}", i);
			versions.push(make_version(i * 10, Some(&value)));
		}
		assert_eq!(versions.inner.len(), 100);
		assert_eq!(versions.inner[0].version, 0);
		assert_eq!(versions.inner[99].version, 990);
	}

	#[test]
	fn test_push_stress_many_updates() {
		let mut versions = Versions::new();
		versions.push(make_version(10, Some("v1")));
		// Update the same version many times (all fast path updates)
		for i in 0..100 {
			let value = format!("v{}", i);
			versions.push(make_version(10, Some(&value)));
		}
		assert_eq!(versions.inner.len(), 1);
		assert_eq!(versions.fetch_version(10), Some(Bytes::from("v99".to_string())));
	}

	// ==================== Tests for gc_older_versions ====================
	//
	// gc_older_versions 的关键不变量：水位线 `floor` 下最新可见的那条版本
	// （若为 tombstone 则整条链可丢）必须留下，任何 snapshot ∈ [floor, +∞)
	// 的读事务都不能因为 GC 而看到不一致的旧值。以下 5 个用例覆盖：
	//   - floor 落在"值 → 删除"的空档（回归用例）；
	//   - floor 落在"值 → 值"的空档；
	//   - floor 恰好命中某条可见值；
	//   - floor 之下最新可见是 tombstone，整链回收；
	//   - floor 比最早版本还小，无可回收。

	// 场景 1：floor 落在 (value, delete) 空档，value 必须保留
	#[test]
	fn test_gc_keeps_version_visible_at_floor() {
		// Regression: the GC floor falls in the gap between a value and a
		// later delete. A reader whose snapshot lands in [floor, delete) must
		// still observe the value, so it must survive GC.
		let mut v = make_versions(vec![(10, Some("v1")), (40, None)]);
		v.gc_older_versions(30);
		// The value visible at 30 (and at 35) must remain readable.
		assert_eq!(v.fetch_version(30), Some(Bytes::from("v1".to_string())));
		assert_eq!(v.fetch_version(35), Some(Bytes::from("v1".to_string())));
		// At/after the delete it is gone.
		assert_eq!(v.fetch_version(40), None);
	}

	// 场景 2：floor 落在 (value, value) 空档，较旧的 value 仍是 floor 处可见值
	#[test]
	fn test_gc_keeps_value_before_newer_version_in_gap() {
		// Floor in the gap between two values: the earlier value is visible at
		// the floor and must survive.
		let mut v = make_versions(vec![(10, Some("v1")), (50, Some("v2"))]);
		v.gc_older_versions(30);
		assert_eq!(v.fetch_version(30), Some(Bytes::from("v1".to_string())));
		assert_eq!(v.fetch_version(49), Some(Bytes::from("v1".to_string())));
		assert_eq!(v.fetch_version(50), Some(Bytes::from("v2".to_string())));
	}

	// 场景 3：floor 恰好命中某条 value，更旧的历史被丢弃、命中的这条保留
	#[test]
	fn test_gc_drops_versions_below_visible() {
		// Floor exactly on a value: older versions are reclaimed, the visible
		// one is kept.
		let mut v = make_versions(vec![(10, Some("v1")), (30, Some("v2"))]);
		assert_eq!(v.fetch_version(10), Some(Bytes::from("v1".to_string())));
		assert_eq!(v.gc_older_versions(30), 1);
		assert_eq!(v.fetch_version(10), None);
		assert_eq!(v.fetch_version(30), Some(Bytes::from("v2".to_string())));
		assert_eq!(v.fetch_version(35), Some(Bytes::from("v2".to_string())));
	}

	// 场景 4：floor 下最新可见是 tombstone，整条版本链都可回收
	#[test]
	fn test_gc_collapses_fully_deleted_key() {
		// Visible entry at the floor is a delete tombstone: the whole chain is
		// reclaimable.
		let mut v = make_versions(vec![(10, Some("v1")), (30, None)]);
		assert_eq!(v.gc_older_versions(40), 0);
		assert_eq!(v.fetch_version(10), None);
		assert_eq!(v.fetch_version(40), None);
	}

	// 场景 5：floor 比最早版本还小（lte == 0），版本链原封不动
	#[test]
	fn test_gc_retains_all_when_floor_below_everything() {
		// Floor below the earliest version: nothing is reclaimable.
		let mut v = make_versions(vec![(10, Some("v1")), (20, Some("v2"))]);
		assert_eq!(v.gc_older_versions(5), 2);
		assert_eq!(v.fetch_version(10), Some(Bytes::from("v1".to_string())));
		assert_eq!(v.fetch_version(20), Some(Bytes::from("v2".to_string())));
	}
}