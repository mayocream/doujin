use std::fs;
use std::path::PathBuf;

use chrono::TimeZone;
use futures::future;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use serde_json::Value;
use sqlx::{PgPool, QueryBuilder, postgres::PgPoolOptions};

// Common progress bar style for all processors
fn create_progress_bar(mp: MultiProgress, total: u64) -> ProgressBar {
    let pb = mp.add(ProgressBar::new(total));
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg} ({eta})",
            )
            .unwrap()
            .progress_chars("#>-"),
    );
    pb
}

// Generic data processor that handles common patterns
async fn process_data<F>(
    query_prefix: &str,
    directory: &str,
    pool: PgPool,
    mp: MultiProgress,
    process_entry: F,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    F: Fn(&mut QueryBuilder<'_, sqlx::Postgres>, &Value, i64) + Send + Sync + Clone + 'static,
{
    // Read and sort entries
    let entries: Vec<PathBuf> = fs::read_dir(directory)?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.to_string_lossy().ends_with(".json"))
        .collect();

    let pb = create_progress_bar(mp, entries.len() as u64);

    // Process in batches of 1000
    for batch in entries.chunks(1000) {
        let mut query_builder = QueryBuilder::new(query_prefix);

        // Process each entry
        for entry in batch {
            // Parse JSON and extract ID
            let content = fs::read_to_string(entry)?;
            let data: Value = serde_json::from_str(&content)?;
            let id = entry
                .file_stem()
                .unwrap()
                .to_str()
                .unwrap()
                .parse::<i64>()?;

            // Let the specific processor handle this entry
            process_entry(&mut query_builder, &data, id);
        }

        // Execute the batch
        query_builder.build().execute(&pool).await?;
        pb.inc(batch.len() as u64);
    }

    pb.finish_with_message("Processing complete");
    Ok(())
}

// Process circle-author relationships
async fn process_circle_relationships(
    directory: &str,
    pool: &PgPool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let entries: Vec<PathBuf> = fs::read_dir(directory)?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.to_string_lossy().ends_with(".json"))
        .collect();

    let mut pairs = Vec::new();

    // Collect all circle-author pairs
    for entry in &entries {
        let content = fs::read_to_string(entry)?;
        let data: Value = serde_json::from_str(&content)?;
        let circle_id = entry
            .file_stem()
            .unwrap()
            .to_str()
            .unwrap()
            .parse::<i64>()?;

        if let Some(authors) = data["LINKS"]["ITEM"].as_array() {
            for author in authors {
                if let Some(author_id) = author["@ID"].as_str() {
                    if let Ok(author_id) = author_id.trim_start_matches("A").parse::<i64>() {
                        pairs.push((circle_id, author_id));
                    }
                }
            }
        }
    }

    // Insert relationships if any exist
    if !pairs.is_empty() {
        let mut query = QueryBuilder::new("INSERT INTO circle_authors (circle_id, author_id) ");
        query.push_values(pairs, |mut b, (circle_id, author_id)| {
            b.push_bind(circle_id).push_bind(author_id);
        });
        query.build().execute(pool).await?;
    }

    Ok(())
}

async fn process_books(
    pool: PgPool,
    mp: MultiProgress,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    process_data(
        "INSERT INTO books (id, type, name, name_en, name_romaji, description, release_date, isbn, pages, language, is_adult, is_anthology, is_copybook, is_magazine) ",
        "./data/doujinshi.org/Book",
        pool,
        mp,
        |query_builder, data, id| {
            let release_date = data["DATE_RELEASED"].as_str().map(|s| {
                chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .unwrap_or_default()
                    .and_hms_opt(0, 0, 0)
                    .map(|dt| chrono::Utc.from_utc_datetime(&dt))
                    .unwrap_or_default()
            });

            query_builder.push_values(std::iter::once(()), |mut b, _| {
                b.push_bind(id)
                    .push_bind(data["@TYPE"].as_str().map(|s| s.to_owned()))
                    .push_bind(data["NAME_JP"].as_str().map(|s| s.to_owned()))
                    .push_bind(data["NAME_EN"].as_str().map(|s| s.to_owned()))
                    .push_bind(data["NAME_R"].as_str().map(|s| s.to_owned()))
                    .push_bind(data["DATA_INFO"].as_str().map(|s| s.to_owned()))
                    .push_bind(release_date)
                    .push_bind(data["DATA_ISBN"].as_str().map(|s| s.to_owned()))
                    .push_bind(data["DATA_PAGES"].as_str().map(|s| s.parse::<i64>().unwrap_or_default()))
                    .push_bind(data["DATA_LANGUAGE"].as_str().map(|s| s.to_owned()))
                    .push_bind(data["DATA_AGE"].as_str() == Some("1"))
                    .push_bind(data["DATA_ANTHOLOGY"].as_str() == Some("1"))
                    .push_bind(data["DATA_COPYSHI"].as_str() == Some("1"))
                    .push_bind(data["DATA_MAGAZINE"].as_str() == Some("1"));
            });
        },
    ).await
}

async fn process_authors(
    pool: PgPool,
    mp: MultiProgress,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    process_data(
        "INSERT INTO authors (id, name, name_en, name_romaji) ",
        "./data/doujinshi.org/Author",
        pool,
        mp,
        |query_builder, data, id| {
            query_builder.push_values(std::iter::once(()), |mut b, _| {
                b.push_bind(id)
                    .push_bind(data["NAME_JP"].as_str().map(|s| s.to_owned()))
                    .push_bind(data["NAME_EN"].as_str().map(|s| s.to_owned()))
                    .push_bind(data["NAME_R"].as_str().map(|s| s.to_owned()));
            });
        },
    )
    .await
}

async fn process_circles(
    pool: PgPool,
    mp: MultiProgress,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Process circle data
    process_data(
        "INSERT INTO circles (id, name, name_en, name_romaji) ",
        "./data/doujinshi.org/Circle",
        pool.clone(),
        mp,
        |query_builder, data, id| {
            query_builder.push_values(std::iter::once(()), |mut b, _| {
                b.push_bind(id)
                    .push_bind(data["NAME_JP"].as_str().map(|s| s.to_owned()))
                    .push_bind(data["NAME_EN"].as_str().map(|s| s.to_owned()))
                    .push_bind(data["NAME_R"].as_str().map(|s| s.to_owned()));
            });
        },
    )
    .await?;

    // Process circle-author relationships
    process_circle_relationships("./data/doujinshi.org/Circle", &pool).await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenvy::dotenv()?;

    let pool = PgPoolOptions::new()
        .max_connections(200)
        .connect(&std::env::var("DATABASE_URL")?)
        .await?;

    let mp = MultiProgress::new();

    let tasks = vec![
        tokio::spawn(process_books(pool.clone(), mp.clone())),
        tokio::spawn(process_authors(pool.clone(), mp.clone())),
        tokio::spawn(process_circles(pool.clone(), mp.clone())),
    ];

    for task in future::join_all(tasks).await {
        task??;
    }

    Ok(())
}
