use std::fs;

use indicatif::{ProgressBar, ProgressStyle};
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv()?;

    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&std::env::var("DATABASE_URL")?)
        .await?;

    let mut entries = fs::read_dir("./data/doujinshi.org/Book")?
        .map(|res| res.map(|e| e.path()))
        .collect::<Result<Vec<_>, std::io::Error>>()?;

    entries.sort();

    println!("Found {} entries", entries.len());

    // Create a single progress bar for overall progress
    let pb = ProgressBar::new(entries.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})",
            )
            .unwrap()
            .progress_chars("#>-"),
    );

    // Split the entries into chunks of 100000
    let chunks = entries.chunks(100_000);

    // Spawn a task for each chunk
    let mut tasks = vec![];
    for chunk in chunks {
        let pool = pool.clone();
        let chunk = chunk.to_vec();
        let pb = pb.clone();

        tasks.push(tokio::spawn(async move {
            for entry in chunk {
                // Skip files that are not JSON
                let path = entry.to_string_lossy();
                if !path.ends_with(".json") {
                    pb.inc(1);
                    continue;
                }

                let data = fs::read_to_string(path.into_owned()).unwrap();
                let data: serde_json::Value = serde_json::from_str(&data).unwrap();

                sqlx::query!(
                   r#"
                    INSERT INTO books (id, type, name, name_en, name_romaji, description, release_date, isbn, pages, language, is_adult, is_anthology, is_copybook, is_magazine)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                   "#,
                    entry.file_stem().unwrap().to_str().unwrap().parse::<i64>().unwrap(),
                    data["@TYPE"].as_str(),
                    data["NAME_JP"].as_str(),
                    data["NAME_EN"].as_str(),
                    data["NAME_R"].as_str(),
                    data["DATA_INFO"].as_str(),
                    data["RELEASE_DATE"].as_str().map(|s| chrono::DateTime::parse_from_rfc3339(s).unwrap_or_default()),
                    data["DATA_ISBN"].as_str(),
                    data["DATA_PAGES"].as_str().map(|s| s.parse::<i64>().unwrap()),
                    data["DATA_LANGUAGE"].as_str(),
                    data["DATA_AGE"].as_str() == Some("1"),
                    data["DATA_ANTHOLOGY"].as_str() == Some("1"),
                    data["DATA_COPYSHI"].as_str() == Some("1"),
                    data["DATA_MAGAZINE"].as_str() == Some("1"),
                )
                .execute(&pool)
                .await
                .ok();

                pb.inc(1);
            }
        }));
    }

    // Wait for all tasks to finish
    for task in tasks {
        if let Err(e) = task.await {
            eprintln!("Task failed: {:?}", e);
        }
    }

    pb.finish_with_message("Processing complete");

    Ok(())
}
