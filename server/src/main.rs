use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, post, put},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use stupid_kv::{Database, error::Error as KvError};

#[derive(Serialize)]
struct GetResponse {
    key: String,
    value: Option<String>,
}

#[derive(Serialize)]
struct ExistsResponse {
    key: String,
    exists: bool,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Deserialize)]
struct WriteRequest {
    value: String,
}

#[derive(Serialize)]
struct WriteResponse {
    key: String,
    value: String,
}

type ApiResult<T> = Result<Json<T>, (StatusCode, Json<ErrorResponse>)>;

fn map_kv_error(err: KvError) -> (StatusCode, Json<ErrorResponse>) {
    let status = match &err {
        KvError::KeyWriteConflict | KvError::KeyReadConflict => StatusCode::CONFLICT,
        KvError::KeyAlreadyExists => StatusCode::CONFLICT,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, Json(ErrorResponse { error: err.to_string() }))
}

async fn get_handler(
    State(db): State<Arc<Database>>,
    Path(key): Path<String>,
) -> ApiResult<GetResponse> {
    let tx = db.transaction(false);
    let value = tx.get(&key).map_err(map_kv_error)?;
    Ok(Json(GetResponse {
        key,
        value: value.map(|v| String::from_utf8_lossy(&v).into_owned()),
    }))
}

async fn exists_handler(
    State(db): State<Arc<Database>>,
    Path(key): Path<String>,
) -> ApiResult<ExistsResponse> {
    let tx = db.transaction(false);
    let exists = tx.exists(&key).map_err(map_kv_error)?;
    Ok(Json(ExistsResponse { key, exists }))
}

async fn post_handler(
    State(db): State<Arc<Database>>,
    Path(key): Path<String>,
    Json(body): Json<WriteRequest>,
) -> ApiResult<WriteResponse> {
    let mut tx = db.transaction(true);
    tx.put(&key, &body.value).map_err(map_kv_error)?;
    tx.commit().map_err(map_kv_error)?;
    Ok(Json(WriteResponse {
        key,
        value: body.value,
    }))
}

async fn put_handler(
    State(db): State<Arc<Database>>,
    Path(key): Path<String>,
    Json(body): Json<WriteRequest>,
) -> ApiResult<WriteResponse> {
    let mut tx = db.transaction(true);
    tx.set(&key, &body.value).map_err(map_kv_error)?;
    tx.commit().map_err(map_kv_error)?;
    Ok(Json(WriteResponse {
        key,
        value: body.value,
    }))
}

async fn delete_handler(
    State(db): State<Arc<Database>>,
    Path(key): Path<String>,
) -> ApiResult<GetResponse> {
    let mut tx = db.transaction(true);
    let value = tx.get(&key).map_err(map_kv_error)?;
    tx.del(&key).map_err(map_kv_error)?;
    tx.commit().map_err(map_kv_error)?;
    Ok(Json(GetResponse {
        key,
        value: value.map(|v| String::from_utf8_lossy(&v).into_owned()),
    }))
}

fn make_app(db: Arc<Database>) -> Router {
    Router::new()
        .route("/{key}", get(get_handler))
        .route("/{key}", post(post_handler))
        .route("/{key}", put(put_handler))
        .route("/{key}", delete(delete_handler))
        .route("/exists/{key}", get(exists_handler))
        .with_state(db)
}

#[tokio::main]
async fn main() {
    let db = Arc::new(Database::new());
    let app = make_app(db);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();

    println!("stupid-kv server listening on http://127.0.0.1:3000");

    axum::serve(listener, app).await.unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn setup() -> Router {
        make_app(Arc::new(Database::new()))
    }

    async fn json_response(res: http::Response<Body>) -> (StatusCode, serde_json::Value) {
        let status = res.status();
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        (status, json)
    }

    #[tokio::test]
    async fn test_post_create_key_successfully() {
        let app = setup();

        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/hello")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"value":"world"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        let (status, json) = json_response(res).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["key"], "hello");
        assert_eq!(json["value"], "world");
    }

    #[tokio::test]
    async fn test_post_duplicate_key_returns_conflict() {
        let app = setup();

        // First create
        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/dup")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"value":"first"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // Second create same key should fail
        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/dup")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"value":"second"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        let (status, json) = json_response(res).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(json["error"].as_str().unwrap().contains("Key already exists"));
    }

    #[tokio::test]
    async fn test_put_upsert_non_existing_key() {
        let app = setup();

        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("PUT")
                    .uri("/newkey")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"value":"newvalue"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        let (status, json) = json_response(res).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["key"], "newkey");
        assert_eq!(json["value"], "newvalue");

        // Verify via GET
        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .uri("/newkey")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, json) = json_response(res).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["value"], "newvalue");
    }

    #[tokio::test]
    async fn test_put_update_existing_key() {
        let app = setup();

        // Create with POST first
        let _ = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/update")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"value":"v1"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Update with PUT
        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("PUT")
                    .uri("/update")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"value":"v2"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        let (status, json) = json_response(res).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["value"], "v2");

        // Verify
        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .uri("/update")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, json) = json_response(res).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["value"], "v2");
    }

    #[tokio::test]
    async fn test_get_existing_key() {
        let app = setup();

        // Create
        let _ = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/existing")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"value":"data"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Get
        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .uri("/existing")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let (status, json) = json_response(res).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["key"], "existing");
        assert_eq!(json["value"], "data");
    }

    #[tokio::test]
    async fn test_get_non_existing_key_returns_null() {
        let app = setup();

        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .uri("/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let (status, json) = json_response(res).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["key"], "nonexistent");
        assert!(json["value"].is_null());
    }

    #[tokio::test]
    async fn test_exists_true_for_existing_key() {
        let app = setup();

        let _ = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/exists_key")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"value":"check"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .uri("/exists/exists_key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let (status, json) = json_response(res).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["key"], "exists_key");
        assert_eq!(json["exists"], true);
    }

    #[tokio::test]
    async fn test_exists_false_for_non_existing_key() {
        let app = setup();

        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .uri("/exists/no_key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let (status, json) = json_response(res).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["key"], "no_key");
        assert_eq!(json["exists"], false);
    }

    #[tokio::test]
    async fn test_delete_existing_key() {
        let app = setup();

        // Create
        let _ = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/del_key")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"value":"remove_me"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Delete
        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("DELETE")
                    .uri("/del_key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let (status, json) = json_response(res).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["key"], "del_key");
        assert_eq!(json["value"], "remove_me");

        // Verify deleted
        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .uri("/del_key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, json) = json_response(res).await;
        assert_eq!(status, StatusCode::OK);
        assert!(json["value"].is_null());
    }

    #[tokio::test]
    async fn test_delete_non_existing_key() {
        let app = setup();

        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("DELETE")
                    .uri("/no_such_key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let (status, json) = json_response(res).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["key"], "no_such_key");
        assert!(json["value"].is_null());
    }

    #[tokio::test]
    async fn test_full_crud_flow() {
        let app = setup();

        // 1. CREATE with POST
        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/flow")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"value":"initial"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // 2. READ with GET
        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .uri("/flow")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, json) = json_response(res).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["value"], "initial");

        // 3. UPDATE with PUT
        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("PUT")
                    .uri("/flow")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"value":"updated"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // 4. Read updated value
        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .uri("/flow")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, json) = json_response(res).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["value"], "updated");

        // 5. EXISTS check
        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .uri("/exists/flow")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, json) = json_response(res).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["exists"], true);

        // 6. DELETE
        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("DELETE")
                    .uri("/flow")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // 7. Verify deleted
        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .uri("/exists/flow")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, json) = json_response(res).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["exists"], false);

        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .uri("/flow")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, json) = json_response(res).await;
        assert_eq!(status, StatusCode::OK);
        assert!(json["value"].is_null());
    }

    #[tokio::test]
    async fn test_mixed_keys_isolation() {
        let app = setup();

        // Create multiple keys
        let keys = ["alpha", "beta", "gamma"];
        for key in &keys {
            let body = serde_json::json!({
                "value": format!("val_{}", key)
            });
            let res = app
                .clone()
                .oneshot(
                    http::Request::builder()
                        .method("POST")
                        .uri(format!("/{}", key))
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_string(&body).unwrap()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::OK);
        }

        // Read all keys
        for key in &keys {
            let res = app
                .clone()
                .oneshot(
                    http::Request::builder()
                        .uri(format!("/{}", key))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            let (status, json) = json_response(res).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(json["value"], format!("val_{}", key));
        }

        // Update one key
        let body = serde_json::json!({
            "value": "val_beta_updated"
        });
        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("PUT")
                    .uri("/beta")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // Verify other keys unchanged
        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .uri("/alpha")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, json) = json_response(res).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["value"], "val_alpha");

        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .uri("/beta")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, json) = json_response(res).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["value"], "val_beta_updated");
    }
}