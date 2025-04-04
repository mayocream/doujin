use std::fs;

use chrono::TimeZone;
use futures::future;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use sqlx::postgres::PgPoolOptions;

async fn process_books(
    mp: MultiProgress,
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut entries = fs::read_dir("./data/doujinshi.org/Book")?
        .map(|res| res.map(|e| e.path()))
        .collect::<Result<Vec<_>, std::io::Error>>()?;

    entries.sort();

    let pb = mp.add(ProgressBar::new(entries.len() as u64));
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})",
            )
            .unwrap(),
    );

    let chunks = entries.chunks(100_000);

    let mut tasks = vec![];
    for chunk in chunks {
        let pool = pool.clone();
        let chunk = chunk.to_vec();
        let pb = pb.clone();

        tasks.push(tokio::spawn(async move {
            for batch in chunk.chunks(1000) {
                let mut query_builder = sqlx::QueryBuilder::new(
                    "INSERT INTO books (id, type, name, name_en, name_romaji, description, release_date, isbn, pages, language, is_adult, is_anthology, is_copybook, is_magazine) "
                );

                query_builder.push_values(batch.iter().filter(|e| e.to_string_lossy().ends_with(".json")), |mut b, entry| {
                    let data = fs::read_to_string(entry.to_string_lossy().into_owned()).unwrap();
                    let data: serde_json::Value = serde_json::from_str(&data).unwrap();
                    let id = entry.file_stem().unwrap().to_str().unwrap().parse::<i64>().unwrap();

                    let release_date = data["DATE_RELEASED"].as_str().map(|s| {
                        chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                            .unwrap_or_default()
                            .and_hms_opt(0, 0, 0)
                            .map(|dt| chrono::Utc.from_utc_datetime(&dt))
                            .unwrap_or_default()
                    });

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

                query_builder.build().execute(&pool).await.ok();
                pb.inc(batch.len() as u64);
            }
        }));
    }

    for task in tasks {
        task.await.ok();
    }

    pb.finish_with_message("Processing complete");

    Ok(())
}

async fn process_authors(
    mp: MultiProgress,
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut entries = fs::read_dir("./data/doujinshi.org/Author")?
        .map(|res| res.map(|e| e.path()))
        .collect::<Result<Vec<_>, std::io::Error>>()?;

    entries.sort();

    let pb = mp.add(ProgressBar::new(entries.len() as u64));
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})",
            )
            .unwrap(),
    );

    let chunks = entries.chunks(100_000);

    let mut tasks = vec![];
    for chunk in chunks {
        let pool = pool.clone();
        let chunk = chunk.to_vec();
        let pb = pb.clone();

        tasks.push(tokio::spawn(async move {
            for batch in chunk.chunks(1000) {
                let mut query_builder = sqlx::QueryBuilder::new(
                    "INSERT INTO authors (id, name, name_en, name_romaji) ",
                );

                query_builder.push_values(
                    batch
                        .iter()
                        .filter(|e| e.to_string_lossy().ends_with(".json")),
                    |mut b, entry| {
                        let data =
                            fs::read_to_string(entry.to_string_lossy().into_owned()).unwrap();
                        let data: serde_json::Value = serde_json::from_str(&data).unwrap();
                        let id = entry
                            .file_stem()
                            .unwrap()
                            .to_str()
                            .unwrap()
                            .parse::<i64>()
                            .unwrap();

                        b.push_bind(id)
                            .push_bind(data["NAME_JP"].as_str().map(|s| s.to_owned()))
                            .push_bind(data["NAME_EN"].as_str().map(|s| s.to_owned()))
                            .push_bind(data["NAME_R"].as_str().map(|s| s.to_owned()));
                    },
                );

                query_builder.build().execute(&pool).await.ok();
                pb.inc(batch.len() as u64);
            }
        }));
    }

    for task in tasks {
        task.await.ok();
    }

    pb.finish_with_message("Processing complete");

    Ok(())
}

async fn process_circles(
    mp: MultiProgress,
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut entries = fs::read_dir("./data/doujinshi.org/Circle")?
        .map(|res| res.map(|e| e.path()))
        .collect::<Result<Vec<_>, std::io::Error>>()?;

    entries.sort();

    let pb = mp.add(ProgressBar::new(entries.len() as u64));
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})",
            )
            .unwrap(),
    );

    let chunks = entries.chunks(100_000);

    let mut tasks = vec![];
    for chunk in chunks {
        let pool = pool.clone();
        let chunk = chunk.to_vec();
        let pb = pb.clone();

        tasks.push(tokio::spawn(async move {
            for batch in chunk.chunks(1000) {
                let mut query_builder = sqlx::QueryBuilder::new(
                    "INSERT INTO circles (id, name, name_en, name_romaji) ",
                );

                query_builder.push_values(
                    batch
                        .iter()
                        .filter(|e| e.to_string_lossy().ends_with(".json")),
                    |mut b, entry| {
                        let data =
                            fs::read_to_string(entry.to_string_lossy().into_owned()).unwrap();
                        let data: serde_json::Value = serde_json::from_str(&data).unwrap();
                        let id = entry
                            .file_stem()
                            .unwrap()
                            .to_str()
                            .unwrap()
                            .parse::<i64>()
                            .unwrap();

                        b.push_bind(id)
                            .push_bind(data["NAME_JP"].as_str().map(|s| s.to_owned()))
                            .push_bind(data["NAME_EN"].as_str().map(|s| s.to_owned()))
                            .push_bind(data["NAME_R"].as_str().map(|s| s.to_owned()));
                    },
                );

                query_builder.build().execute(&pool).await.ok();

                // Process circle_authors relationships using batch insertion
                let mut author_query_builder =
                    sqlx::QueryBuilder::new("INSERT INTO circle_authors (circle_id, author_id) ");

                // Create a vector to collect all circle_id -> author_id pairs
                let mut circle_author_pairs = Vec::new();

                for entry in batch
                    .iter()
                    .filter(|e| e.to_string_lossy().ends_with(".json"))
                {
                    let data = fs::read_to_string(entry.to_string_lossy().into_owned()).unwrap();
                    let data: serde_json::Value = serde_json::from_str(&data).unwrap();

                    let circle_id = entry
                        .file_stem()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .parse::<i64>()
                        .unwrap();

                    // Only process if we have author links
                    if let Some(authors) = data["LINKS"]["ITEM"].as_array() {
                        for author in authors {
                            if let Some(author_id) = author["@ID"].as_str() {
                                let author_id = author_id.trim_start_matches("A");
                                if let Ok(author_id) = author_id.parse::<i64>() {
                                    // Collect the circle_id -> author_id pair
                                    circle_author_pairs.push((circle_id, author_id));
                                }
                            }
                        }
                    }
                }

                // Only build and execute the query if we have pairs to insert
                if !circle_author_pairs.is_empty() {
                    author_query_builder.push_values(
                        circle_author_pairs,
                        |mut b, (circle_id, author_id)| {
                            b.push_bind(circle_id).push_bind(author_id);
                        },
                    );

                    author_query_builder.build().execute(&pool).await.ok();
                }

                pb.inc(batch.len() as u64);
            }
        }));
    }

    for task in tasks {
        task.await.ok();
    }

    pb.finish_with_message("Processing complete");

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
        tokio::spawn(process_books(mp.clone(), pool.clone())),
        tokio::spawn(process_authors(mp.clone(), pool.clone())),
        tokio::spawn(process_circles(mp.clone(), pool.clone())),
    ];

    future::join_all(tasks).await;

    Ok(())
}
