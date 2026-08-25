use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::{delete, get, post},
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
struct KeyQuery {
    key: String,
}

#[derive(Deserialize)]
struct WriteRequest {
    key: String,
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
    Query(params): Query<KeyQuery>,
) -> ApiResult<GetResponse> {
    let key = params.key;
    let tx = db.transaction(false);
    let value = tx.get(&key).map_err(map_kv_error)?;
    Ok(Json(GetResponse {
        key,
        value: value.map(|v| String::from_utf8_lossy(&v).into_owned()),
    }))
}

async fn exists_handler(
    State(db): State<Arc<Database>>,
    Query(params): Query<KeyQuery>,
) -> ApiResult<ExistsResponse> {
    let key = params.key;
    let tx = db.transaction(false);
    let exists = tx.exists(&key).map_err(map_kv_error)?;
    Ok(Json(ExistsResponse { key, exists }))
}

async fn insert_handler(
    State(db): State<Arc<Database>>,
    Json(body): Json<WriteRequest>,
) -> ApiResult<WriteResponse> {
    let mut tx = db.transaction(true);
    tx.put(&body.key, &body.value).map_err(map_kv_error)?;
    tx.commit().map_err(map_kv_error)?;
    Ok(Json(WriteResponse {
        key: body.key,
        value: body.value,
    }))
}

async fn update_handler(
    State(db): State<Arc<Database>>,
    Json(body): Json<WriteRequest>,
) -> ApiResult<WriteResponse> {
    let mut tx = db.transaction(true);
    tx.set(&body.key, &body.value).map_err(map_kv_error)?;
    tx.commit().map_err(map_kv_error)?;
    Ok(Json(WriteResponse {
        key: body.key,
        value: body.value,
    }))
}

async fn delete_handler(
    State(db): State<Arc<Database>>,
    Query(params): Query<KeyQuery>,
) -> ApiResult<GetResponse> {
    let key = params.key;
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
        .route("/get", get(get_handler))
        .route("/insert", post(insert_handler))
        .route("/update", post(update_handler))
        .route("/delete", delete(delete_handler))
        .route("/exists", get(exists_handler))
        .with_state(db)
}

fn parse_port() -> u16 {
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--port" => {
                if i + 1 < args.len() {
                    if let Ok(port) = args[i + 1].parse::<u16>() {
                        return port;
                    }
                }
            }
            "--help" => {
                eprintln!("stupid-kv HTTP Server");
                eprintln!();
                eprintln!("Usage:");
                eprintln!("  PORT=<port> cargo run -p server          # via env var (recommended)");
                eprintln!("  cargo run -p server -- --port <port>     # via CLI arg");
                eprintln!();
                eprintln!("Options:");
                eprintln!("  --port <port>   Set the listening port (default: 3000)");
                eprintln!("  --help          Print this help message");
                std::process::exit(0);
            }
            _ => {}
        }
        i += 1;
    }
    std::env::var("PORT")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(3000)
}

#[tokio::main]
async fn main() {
    let port = parse_port();
    let db = Arc::new(Database::new());
    let app = make_app(db);

    let addr = format!("127.0.0.1:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();

    println!("stupid-kv server listening on http://{addr}");

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
    async fn test_insert_key_successfully() {
        let app = setup();

        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/insert")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"key":"hello","value":"world"}"#))
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
    async fn test_insert_duplicate_key_returns_conflict() {
        let app = setup();

        let body = r#"{"key":"dup","value":"first"}"#;
        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/insert")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let body = r#"{"key":"dup","value":"second"}"#;
        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/insert")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        let (status, json) = json_response(res).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(json["error"].as_str().unwrap().contains("Key already exists"));
    }

    #[tokio::test]
    async fn test_update_non_existing_key() {
        let app = setup();

        let body = r#"{"key":"newkey","value":"newvalue"}"#;
        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/update")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        let (status, json) = json_response(res).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["key"], "newkey");
        assert_eq!(json["value"], "newvalue");

        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .uri("/get?key=newkey")
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
    async fn test_update_existing_key() {
        let app = setup();

        let body = r#"{"key":"update","value":"v1"}"#;
        let _ = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/insert")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = r#"{"key":"update","value":"v2"}"#;
        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/update")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        let (status, json) = json_response(res).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["value"], "v2");

        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .uri("/get?key=update")
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

        let body = r#"{"key":"existing","value":"data"}"#;
        let _ = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/insert")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .uri("/get?key=existing")
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
                    .uri("/get?key=nonexistent")
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

        let body = r#"{"key":"exists_key","value":"check"}"#;
        let _ = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/insert")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .uri("/exists?key=exists_key")
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
                    .uri("/exists?key=no_key")
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

        let body = r#"{"key":"del_key","value":"remove_me"}"#;
        let _ = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/insert")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("DELETE")
                    .uri("/delete?key=del_key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let (status, json) = json_response(res).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["key"], "del_key");
        assert_eq!(json["value"], "remove_me");

        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .uri("/get?key=del_key")
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
                    .uri("/delete?key=no_such_key")
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

        let body = r#"{"key":"flow","value":"initial"}"#;
        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/insert")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .uri("/get?key=flow")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, json) = json_response(res).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["value"], "initial");

        let body = r#"{"key":"flow","value":"updated"}"#;
        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/update")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .uri("/get?key=flow")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, json) = json_response(res).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["value"], "updated");

        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .uri("/exists?key=flow")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, json) = json_response(res).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["exists"], true);

        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("DELETE")
                    .uri("/delete?key=flow")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .uri("/exists?key=flow")
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
                    .uri("/get?key=flow")
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

        let keys = ["alpha", "beta", "gamma"];
        for key in &keys {
            let body = serde_json::json!({
                "key": key,
                "value": format!("val_{}", key)
            });
            let res = app
                .clone()
                .oneshot(
                    http::Request::builder()
                        .method("POST")
                        .uri("/insert")
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_string(&body).unwrap()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::OK);
        }

        for key in &keys {
            let res = app
                .clone()
                .oneshot(
                    http::Request::builder()
                        .uri(format!("/get?key={}", key))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            let (status, json) = json_response(res).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(json["value"], format!("val_{}", key));
        }

        let body = serde_json::json!({
            "key": "beta",
            "value": "val_beta_updated"
        });
        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/update")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .uri("/get?key=alpha")
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
                    .uri("/get?key=beta")
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
