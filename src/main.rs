use warp::Filter;
use warp::ws::{Message, WebSocket};
use tokio::sync::broadcast;
use sqlx::postgres::PgPoolOptions;
use std::env;
use sqlx::Executor; // For executing raw SQL queries
use serde_json::json; // For JSON serialization
use sqlx::Row; // For fetching rows from the database
use futures::{StreamExt, SinkExt}; // For WebSocket split method

async fn init_database(pool: &sqlx::PgPool) {
    // Create the headlines table if it doesn't exist
    pool.execute(
        "CREATE TABLE IF NOT EXISTS headlines (
            id SERIAL PRIMARY KEY,
            content TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );"
    ).await.expect("Failed to create headlines table");

    // Insert some dummy news for testing
    pool.execute(
        "INSERT INTO headlines (content) VALUES
            ('NÓNG: Sửa đổi một số quy định trong lĩnh vực chứng khoán, sẵn sàng go-live KRX trong tháng 5/2025'),
            ('Tôi quyết định bán vàng ở vùng 120 triệu đồng/lượng để đầu tư chứng khoán'),
            ('PCập nhật BCTC quý 1/2025 sáng ngày 21/4: LPBank, Dược Hậu Giang công bố, công ty nước lãi gấp 3,5 lần cùng kỳ ')
        ON CONFLICT DO NOTHING;"
    ).await.expect("Failed to insert dummy news");
}

async fn broadcast_headlines(pool: &sqlx::PgPool, tx: broadcast::Sender<String>) {
    // Query the database for all headlines
    let rows = sqlx::query("SELECT content, created_at FROM headlines ORDER BY created_at DESC")
        .fetch_all(pool)
        .await
        .expect("Failed to fetch headlines from the database");

    // Broadcast each headline to the WebSocket clients
    for row in rows {
        let content: String = row.get("content");
        //let created_at: chrono::DateTime<chrono::Utc> = row.get::<String, _>("created_at")
         //   .parse()
        //    .expect("Failed to parse timestamp");

        let message = json!({
            "headline": content,
            "timestamp": "Today"
        });

        if tx.send(message.to_string()).is_err() {
            eprintln!("Failed to broadcast headline");
        }
    }
}

#[tokio::main]
async fn main() {
    // Database connection
    let database_url = "postgres://postgres:123@localhost/postgres";
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await
        .expect("Failed to connect to the database");

    // Initialize the database
    init_database(&pool).await;

    // Create a broadcast channel for sending headlines
    let (tx, _rx) = broadcast::channel(100);

    // Broadcast existing headlines to clients
    broadcast_headlines(&pool, tx.clone()).await;

    // WebSocket route
    let ws_route = warp::path("ws")
        .and(warp::ws())
        .and(warp::any().map(move || tx.clone()))
        .map(|ws: warp::ws::Ws, tx| {
            ws.on_upgrade(move |socket| handle_connection(socket, tx))
        });

    // Start the server
    warp::serve(ws_route).run(([0, 0, 0, 0], 3030)).await;
}

async fn handle_connection(ws: WebSocket, tx: broadcast::Sender<String>) {
    let (mut ws_tx, mut ws_rx) = ws.split();
    let mut rx = tx.subscribe();

    // Spawn a task to send messages to the client
    tokio::spawn(async move {
        while let Ok(headline) = rx.recv().await {
            // Simulate a JSON structure with headline and timestamp
            let message = json!({
                "headline": headline,
                "timestamp": chrono::Utc::now().to_rfc3339()
            });

            if ws_tx.send(Message::text(message.to_string())).await.is_err() {
                break;
            }
        }
    });

    // Handle incoming messages (optional, for now we just ignore them)
    while ws_rx.next().await.is_some() {}
}
