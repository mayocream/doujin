use std::{path::PathBuf, sync::LazyLock};

use anyhow::Result;
use futures::future;
use glob::glob;
use indicatif::{MultiProgress, ParallelProgressIterator, ProgressBar, ProgressIterator};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

static DATA_DIR: LazyLock<PathBuf> = LazyLock::new(|| PathBuf::from("data/doujinshi.org"));
static MULTT_PROGRESS: LazyLock<MultiProgress> = LazyLock::new(MultiProgress::new);

fn progress_bar() -> ProgressBar {
    MULTT_PROGRESS
        .add(ProgressBar::new_spinner().with_style(indicatif::ProgressStyle::default_bar()))
}

async fn process<F>(pattern: &str, output_file: &str, header: Vec<&str>, transform: F) -> Result<()>
where
    F: Fn(&serde_json::Value) -> Vec<String> + Send + Sync + 'static,
{
    let mut writer = csv::Writer::from_path(output_file)?;
    writer.write_record(&header)?;

    glob(pattern)?
        .filter_map(Result::ok)
        .collect::<Vec<_>>()
        .par_iter()
        .progress_with(
            progress_bar().with_message(format!("Processing files matching pattern: {}", pattern)),
        )
        .map(|file| {
            let file = std::fs::File::open(file).unwrap();
            let reader = std::io::BufReader::new(file);
            let json: serde_json::Value = serde_json::from_reader(reader).unwrap();
            transform(&json)
        })
        .collect::<Vec<_>>()
        .into_iter()
        .filter(|record| !record.is_empty())
        .progress_with(progress_bar().with_message("Writing records"))
        .for_each(|record| {
            writer.write_record(&record).unwrap();
        });

    writer.flush()?;

    Ok(())
}

async fn process_authors() {
    process(
        &DATA_DIR.join("Author").join("*.json").to_string_lossy(),
        &DATA_DIR.join("authors.csv").to_string_lossy(),
        vec!["id", "name", "name_en", "name_romaji", "name_alt"],
        |json| {
            vec![
                json["@ID"].as_str().unwrap_or_default().replace("A", ""),
                json["NAME_JP"].as_str().unwrap_or_default().to_string(),
                json["NAME_EN"].as_str().unwrap_or_default().to_string(),
                json["NAME_R"].as_str().unwrap_or_default().to_string(),
                json["NAME_ALT"]
                    .as_array()
                    .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())
                    .unwrap_or_else(|| vec![json["NAME_ALT"].as_str().unwrap_or_default()])
                    .join(","),
            ]
        },
    )
    .await
    .unwrap()
}

async fn process_characters() {
    process(
        &DATA_DIR.join("Character").join("*.json").to_string_lossy(),
        &DATA_DIR.join("characters.csv").to_string_lossy(),
        vec!["id", "name", "name_en", "name_romaji", "name_alt"],
        |json| {
            vec![
                json["@ID"].as_str().unwrap_or_default().replace("H", ""),
                json["NAME_JP"].as_str().unwrap_or_default().to_string(),
                json["NAME_EN"].as_str().unwrap_or_default().to_string(),
                json["NAME_R"].as_str().unwrap_or_default().to_string(),
                json["NAME_ALT"]
                    .as_array()
                    .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())
                    .unwrap_or_else(|| vec![json["NAME_ALT"].as_str().unwrap_or_default()])
                    .join(","),
            ]
        },
    )
    .await
    .unwrap()
}

async fn process_character_tags() {
    process(
        &DATA_DIR.join("Character").join("*.json").to_string_lossy(),
        &DATA_DIR.join("character_tags.csv").to_string_lossy(),
        vec!["character_id", "tag_id"],
        |json| {
            let tags = json["LINKS"]["ITEM"]
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.get("@ID").unwrap().as_str())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_else(|| match json["LINKS"]["ITEM"].as_str() {
                    Some(tag) => vec![tag],
                    None => vec![],
                })
                .into_iter()
                .map(|tag| tag.replace("K", ""))
                .collect::<Vec<_>>();

            // Skip if no tags are found
            if tags.is_empty() {
                return vec![];
            }

            vec![
                json["@ID"].as_str().unwrap_or_default().replace("H", ""),
                tags.join(","),
            ]
        },
    )
    .await
    .unwrap()
}

async fn process_tags() {
    process(
        &DATA_DIR.join("Content").join("*.json").to_string_lossy(),
        &DATA_DIR.join("tags.csv").to_string_lossy(),
        vec!["id", "name", "name_en", "name_romaji", "name_alt"],
        |json| {
            vec![
                json["@ID"].as_str().unwrap_or_default().replace("K", ""),
                json["NAME_JP"].as_str().unwrap_or_default().to_string(),
                json["NAME_EN"].as_str().unwrap_or_default().to_string(),
                json["NAME_R"].as_str().unwrap_or_default().to_string(),
                json["NAME_ALT"]
                    .as_array()
                    .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())
                    .unwrap_or_else(|| vec![json["NAME_ALT"].as_str().unwrap_or_default()])
                    .join(","),
            ]
        },
    )
    .await
    .unwrap()
}

async fn process_conventions() {
    process(
        &DATA_DIR.join("Convention").join("*.json").to_string_lossy(),
        &DATA_DIR.join("conventions.csv").to_string_lossy(),
        vec![
            "id",
            "name",
            "name_en",
            "name_romaji",
            "name_alt",
            "start_date",
            "end_date",
        ],
        |json| {
            vec![
                json["@ID"].as_str().unwrap_or_default().replace("C", ""),
                json["NAME_JP"].as_str().unwrap_or_default().to_string(),
                json["NAME_EN"].as_str().unwrap_or_default().to_string(),
                json["NAME_R"].as_str().unwrap_or_default().to_string(),
                json["NAME_ALT"]
                    .as_array()
                    .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())
                    .unwrap_or_else(|| vec![json["NAME_ALT"].as_str().unwrap_or_default()])
                    .join(","),
                json["DATE_START"].as_str().unwrap_or_default().to_string(),
                json["DATE_END"].as_str().unwrap_or_default().to_string(),
            ]
        },
    )
    .await
    .unwrap()
}

async fn process_genres() {
    process(
        &DATA_DIR.join("Genre").join("*.json").to_string_lossy(),
        &DATA_DIR.join("genres.csv").to_string_lossy(),
        vec!["id", "name", "name_en", "name_romaji", "name_alt"],
        |json| {
            vec![
                json["@ID"].as_str().unwrap_or_default().replace("G", ""),
                json["NAME_JP"].as_str().unwrap_or_default().to_string(),
                json["NAME_EN"].as_str().unwrap_or_default().to_string(),
                json["NAME_R"].as_str().unwrap_or_default().to_string(),
                json["NAME_ALT"]
                    .as_array()
                    .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())
                    .unwrap_or_else(|| vec![json["NAME_ALT"].as_str().unwrap_or_default()])
                    .join(","),
            ]
        },
    )
    .await
    .unwrap()
}

async fn process_series() {
    process(
        &DATA_DIR
            .join("Collections")
            .join("*.json")
            .to_string_lossy(),
        &DATA_DIR.join("series.csv").to_string_lossy(),
        vec!["id", "name", "name_en", "name_romaji", "name_alt"],
        |json| {
            vec![
                json["@ID"].as_str().unwrap_or_default().replace("O", ""),
                json["NAME_JP"].as_str().unwrap_or_default().to_string(),
                json["NAME_EN"].as_str().unwrap_or_default().to_string(),
                json["NAME_R"].as_str().unwrap_or_default().to_string(),
                json["NAME_ALT"]
                    .as_array()
                    .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())
                    .unwrap_or_else(|| vec![json["NAME_ALT"].as_str().unwrap_or_default()])
                    .join(","),
            ]
        },
    )
    .await
    .unwrap()
}

async fn process_imprints() {
    process(
        &DATA_DIR.join("Imprint").join("*.json").to_string_lossy(),
        &DATA_DIR.join("imprints.csv").to_string_lossy(),
        vec!["id", "name", "name_en", "name_romaji", "name_alt"],
        |json| {
            vec![
                json["@ID"].as_str().unwrap_or_default().replace("I", ""),
                json["NAME_JP"].as_str().unwrap_or_default().to_string(),
                json["NAME_EN"].as_str().unwrap_or_default().to_string(),
                json["NAME_R"].as_str().unwrap_or_default().to_string(),
                json["NAME_ALT"]
                    .as_array()
                    .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())
                    .unwrap_or_else(|| vec![json["NAME_ALT"].as_str().unwrap_or_default()])
                    .join(","),
            ]
        },
    )
    .await
    .unwrap()
}

async fn process_parodies() {
    process(
        &DATA_DIR.join("Parody").join("*.json").to_string_lossy(),
        &DATA_DIR.join("parodies.csv").to_string_lossy(),
        vec!["id", "name", "name_en", "name_romaji", "name_alt"],
        |json| {
            vec![
                json["@ID"].as_str().unwrap_or_default().replace("P", ""),
                json["NAME_JP"].as_str().unwrap_or_default().to_string(),
                json["NAME_EN"].as_str().unwrap_or_default().to_string(),
                json["NAME_R"].as_str().unwrap_or_default().to_string(),
                json["NAME_ALT"]
                    .as_array()
                    .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())
                    .unwrap_or_else(|| vec![json["NAME_ALT"].as_str().unwrap_or_default()])
                    .join(","),
            ]
        },
    )
    .await
    .unwrap()
}

async fn process_parody_characters() {
    process(
        &DATA_DIR.join("Parody").join("*.json").to_string_lossy(),
        &DATA_DIR.join("parody_characters.csv").to_string_lossy(),
        vec!["parody_id", "character_id"],
        |json| {
            let tags = json["LINKS"]["ITEM"]
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.get("@ID").unwrap().as_str())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_else(|| match json["LINKS"]["ITEM"].as_str() {
                    Some(tag) => vec![tag],
                    None => vec![],
                })
                .into_iter()
                .filter(|tag| tag.starts_with("H"))
                .map(|tag| tag.replace("H", ""))
                .collect::<Vec<_>>();

            // Skip if no tags are found
            if tags.is_empty() {
                return vec![];
            }

            vec![
                json["@ID"].as_str().unwrap_or_default().replace("P", ""),
                tags.join(","),
            ]
        },
    )
    .await
    .unwrap()
}

async fn process_parody_tags() {
    process(
        &DATA_DIR.join("Parody").join("*.json").to_string_lossy(),
        &DATA_DIR.join("parody_tags.csv").to_string_lossy(),
        vec!["parody_id", "tag_id"],
        |json| {
            let tags = json["LINKS"]["ITEM"]
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.get("@ID").unwrap().as_str())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_else(|| match json["LINKS"]["ITEM"].as_str() {
                    Some(tag) => vec![tag],
                    None => vec![],
                })
                .into_iter()
                .filter(|tag| tag.starts_with("K"))
                .map(|tag| tag.replace("K", ""))
                .collect::<Vec<_>>();

            // Skip if no tags are found
            if tags.is_empty() {
                return vec![];
            }

            vec![
                json["@ID"].as_str().unwrap_or_default().replace("P", ""),
                tags.join(","),
            ]
        },
    )
    .await
    .unwrap()
}

async fn process_publishers() {
    process(
        &DATA_DIR.join("Publisher").join("*.json").to_string_lossy(),
        &DATA_DIR.join("publishers.csv").to_string_lossy(),
        vec!["id", "name", "name_en", "name_romaji", "name_alt"],
        |json| {
            vec![
                json["@ID"].as_str().unwrap_or_default().replace("B", ""),
                json["NAME_JP"].as_str().unwrap_or_default().to_string(),
                json["NAME_EN"].as_str().unwrap_or_default().to_string(),
                json["NAME_R"].as_str().unwrap_or_default().to_string(),
                json["NAME_ALT"]
                    .as_array()
                    .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())
                    .unwrap_or_else(|| vec![json["NAME_ALT"].as_str().unwrap_or_default()])
                    .join(","),
            ]
        },
    )
    .await
    .unwrap()
}

async fn process_types() {
    process(
        &DATA_DIR.join("Type").join("*.json").to_string_lossy(),
        &DATA_DIR.join("types.csv").to_string_lossy(),
        vec!["id", "name", "name_en", "name_romaji", "name_alt"],
        |json| {
            vec![
                json["@ID"].as_str().unwrap_or_default().replace("T", ""),
                json["NAME_JP"].as_str().unwrap_or_default().to_string(),
                json["NAME_EN"].as_str().unwrap_or_default().to_string(),
                json["NAME_R"].as_str().unwrap_or_default().to_string(),
                json["NAME_ALT"]
                    .as_array()
                    .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())
                    .unwrap_or_else(|| vec![json["NAME_ALT"].as_str().unwrap_or_default()])
                    .join(","),
            ]
        },
    )
    .await
    .unwrap()
}

async fn process_books() {
    process(
        &DATA_DIR.join("Book").join("*.json").to_string_lossy(),
        &DATA_DIR.join("books.csv").to_string_lossy(),
        vec!["id", "name", "name_en", "name_romaji", "name_alt"],
        |json| {
            vec![
                json["@ID"].as_str().unwrap_or_default().replace("B", ""),
                json["NAME_JP"].as_str().unwrap_or_default().to_string(),
                json["NAME_EN"].as_str().unwrap_or_default().to_string(),
                json["NAME_R"].as_str().unwrap_or_default().to_string(),
                json["NAME_ALT"]
                    .as_array()
                    .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())
                    .unwrap_or_else(|| vec![json["NAME_ALT"].as_str().unwrap_or_default()])
                    .join(","),
            ]
        },
    )
    .await
    .unwrap()
}

async fn process_book_authors() {
    process(
        &DATA_DIR.join("Book").join("*.json").to_string_lossy(),
        &DATA_DIR.join("book_authors.csv").to_string_lossy(),
        vec!["book_id", "author_id"],
        |json| {
            let authors = json["LINKS"]["ITEM"]
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.get("@ID").unwrap().as_str())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_else(|| match json["LINKS"]["ITEM"].as_str() {
                    Some(tag) => vec![tag],
                    None => vec![],
                })
                .into_iter()
                .filter(|tag| tag.starts_with("A"))
                .map(|tag| tag.replace("A", ""))
                .collect::<Vec<_>>();

            // Skip if no tags are found
            if authors.is_empty() {
                return vec![];
            }

            vec![
                json["@ID"].as_str().unwrap_or_default().replace("B", ""),
                authors.join(","),
            ]
        },
    )
    .await
    .unwrap()
}

async fn process_book_characters() {
    process(
        &DATA_DIR.join("Book").join("*.json").to_string_lossy(),
        &DATA_DIR.join("book_characters.csv").to_string_lossy(),
        vec!["book_id", "character_id"],
        |json| {
            let characters = json["LINKS"]["ITEM"]
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.get("@ID").unwrap().as_str())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_else(|| match json["LINKS"]["ITEM"].as_str() {
                    Some(tag) => vec![tag],
                    None => vec![],
                })
                .into_iter()
                .filter(|tag| tag.starts_with("H"))
                .map(|tag| tag.replace("H", ""))
                .collect::<Vec<_>>();

            // Skip if no tags are found
            if characters.is_empty() {
                return vec![];
            }

            vec![
                json["@ID"].as_str().unwrap_or_default().replace("B", ""),
                characters.join(","),
            ]
        },
    )
    .await
    .unwrap()
}

async fn process_book_tags() {
    process(
        &DATA_DIR.join("Book").join("*.json").to_string_lossy(),
        &DATA_DIR.join("book_tags.csv").to_string_lossy(),
        vec!["book_id", "tag_id"],
        |json| {
            let tags = json["LINKS"]["ITEM"]
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.get("@ID").unwrap().as_str())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_else(|| match json["LINKS"]["ITEM"].as_str() {
                    Some(tag) => vec![tag],
                    None => vec![],
                })
                .into_iter()
                .filter(|tag| tag.starts_with("K"))
                .map(|tag| tag.replace("K", ""))
                .collect::<Vec<_>>();

            // Skip if no tags are found
            if tags.is_empty() {
                return vec![];
            }

            vec![
                json["@ID"].as_str().unwrap_or_default().replace("B", ""),
                tags.join(","),
            ]
        },
    )
    .await
    .unwrap()
}

#[tokio::main]
async fn main() {
    future::join_all(vec![
        tokio::spawn(process_authors()),
        tokio::spawn(process_characters()),
        tokio::spawn(process_character_tags()),
        tokio::spawn(process_tags()),
        tokio::spawn(process_conventions()),
        tokio::spawn(process_genres()),
        tokio::spawn(process_series()),
        tokio::spawn(process_imprints()),
        tokio::spawn(process_parodies()),
        tokio::spawn(process_parody_characters()),
        tokio::spawn(process_parody_tags()),
        tokio::spawn(process_publishers()),
        tokio::spawn(process_types()),
        tokio::spawn(process_books()),
        tokio::spawn(process_book_authors()),
        tokio::spawn(process_book_characters()),
        tokio::spawn(process_book_tags()),
    ])
    .await;

    println!("Processing completed.")
}
