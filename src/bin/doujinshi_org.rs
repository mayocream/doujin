use anyhow::Result;
use futures::future;
use glob::glob;
use indicatif::{MultiProgress, ParallelProgressIterator, ProgressBar};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use serde_json::Value;
use std::{path::PathBuf, sync::LazyLock};

static DATA_DIR: LazyLock<PathBuf> = LazyLock::new(|| PathBuf::from("data/doujinshi.org"));
static MULTI_PROGRESS: LazyLock<MultiProgress> = LazyLock::new(MultiProgress::new);

fn progress_bar(len: u64) -> ProgressBar {
    MULTI_PROGRESS.add(ProgressBar::new(len).with_style(indicatif::ProgressStyle::default_bar()))
}

fn get_str(json: &Value, key: &str) -> String {
    json[key].as_str().unwrap_or_default().to_string()
}

fn extract_common_fields(json: &Value, id_prefix: &str) -> Vec<String> {
    vec![
        get_str(json, "@ID").replace(id_prefix, ""),
        get_str(json, "NAME_JP"),
        get_str(json, "NAME_EN"),
        get_str(json, "NAME_R"),
        process_alt_names(json),
    ]
}

fn extract_links(json: &Value) -> Vec<String> {
    json["LINKS"]["ITEM"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.get("@ID").unwrap().as_str().map(String::from))
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| match json["LINKS"]["ITEM"].as_str() {
            Some(tag) => vec![tag.to_string()],
            None => vec![],
        })
}

fn filter_links(links: &[String], prefix: &str) -> String {
    links
        .iter()
        .filter(|tag| tag.starts_with(prefix))
        .map(|tag| tag.replace(prefix, ""))
        .collect::<Vec<_>>()
        .join(",")
}

fn process_alt_names(json: &Value) -> String {
    json["NAME_ALT"]
        .as_array()
        .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())
        .unwrap_or_else(|| vec![json["NAME_ALT"].as_str().unwrap_or_default()])
        .join(",")
}

struct Entity {
    dir_name: &'static str,
    csv_name: &'static str,
    headers: Vec<&'static str>,
}

// Main processing function
async fn process_entity<F>(entity: Entity, transform: F) -> Result<()>
where
    F: Fn(&Value) -> Vec<Vec<String>> + Send + Sync + 'static,
{
    let pattern = DATA_DIR
        .join(entity.dir_name)
        .join("*.json")
        .to_string_lossy()
        .to_string();

    let output_file = DATA_DIR
        .join(format!("{}.csv", entity.csv_name))
        .to_string_lossy()
        .to_string();

    let mut writer = csv::WriterBuilder::new()
        .delimiter(b'\t')
        .from_path(&output_file)?;
    writer.write_record(&entity.headers)?;

    glob(&pattern)?
        .filter_map(Result::ok)
        .collect::<Vec<_>>()
        .par_iter()
        .progress_with(progress_bar(glob(&pattern)?.count() as u64))
        .flat_map(|file| {
            let json: Value = serde_json::from_reader(std::io::BufReader::new(
                std::fs::File::open(file).unwrap(),
            ))
            .unwrap();
            transform(&json)
        })
        .collect::<Vec<_>>()
        .into_iter()
        .filter(|record| !record.is_empty())
        .for_each(|record| {
            // Replace empty strings with \N
            let record: Vec<String> = record
                .into_iter()
                .map(|s| s.replace("\n", ""))
                .map(|s| if s.is_empty() { "\\N".to_string() } else { s })
                .collect();
            // Write to CSV at once to avoid using Mutex
            writer.write_record(&record).unwrap();
        });

    writer.flush()?;
    Ok(())
}

fn relationship_transform(
    json: &Value,
    source_prefix: &str,
    target_prefix: &str,
) -> Vec<Vec<String>> {
    let links = extract_links(json);
    let source_id = get_str(json, "@ID").replace(source_prefix, "");

    let related_items: Vec<String> = links
        .into_iter()
        .filter(|tag| tag.starts_with(target_prefix))
        .map(|tag| tag.replace(target_prefix, ""))
        .collect();

    if related_items.is_empty() {
        return vec![];
    }

    related_items
        .into_iter()
        .map(|target_id| vec![source_id.clone(), target_id])
        .collect()
}

#[tokio::main]
async fn main() {
    let tasks = vec![
        // Standard entities
        tokio::spawn(process_entity(
            Entity {
                dir_name: "Author",
                csv_name: "authors",
                headers: vec!["id", "name", "name_en", "name_romaji", "name_alt"],
            },
            move |json| vec![extract_common_fields(json, "A")],
        )),
        tokio::spawn(process_entity(
            Entity {
                dir_name: "Character",
                csv_name: "characters",
                headers: vec!["id", "name", "name_en", "name_romaji", "name_alt"],
            },
            move |json| vec![extract_common_fields(json, "H")],
        )),
        tokio::spawn(process_entity(
            Entity {
                dir_name: "Content",
                csv_name: "tags",
                headers: vec!["id", "name", "name_en", "name_romaji", "name_alt"],
            },
            move |json| vec![extract_common_fields(json, "K")],
        )),
        tokio::spawn(process_entity(
            Entity {
                dir_name: "Circle",
                csv_name: "circles",
                headers: vec!["id", "name", "name_en", "name_romaji", "name_alt"],
            },
            move |json| vec![extract_common_fields(json, "C")],
        )),
        tokio::spawn(process_entity(
            Entity {
                dir_name: "Genre",
                csv_name: "genres",
                headers: vec!["id", "name", "name_en", "name_romaji", "name_alt"],
            },
            move |json| vec![extract_common_fields(json, "G")],
        )),
        tokio::spawn(process_entity(
            Entity {
                dir_name: "Collections",
                csv_name: "magazines",
                headers: vec!["id", "name", "name_en", "name_romaji", "name_alt"],
            },
            move |json| vec![extract_common_fields(json, "O")],
        )),
        tokio::spawn(process_entity(
            Entity {
                dir_name: "Imprint",
                csv_name: "imprints",
                headers: vec!["id", "name", "name_en", "name_romaji", "name_alt"],
            },
            move |json| vec![extract_common_fields(json, "I")],
        )),
        tokio::spawn(process_entity(
            Entity {
                dir_name: "Parody",
                csv_name: "parodies",
                headers: vec!["id", "name", "name_en", "name_romaji", "name_alt"],
            },
            move |json| vec![extract_common_fields(json, "P")],
        )),
        tokio::spawn(process_entity(
            Entity {
                dir_name: "Publisher",
                csv_name: "publishers",
                headers: vec!["id", "name", "name_en", "name_romaji", "name_alt"],
            },
            move |json| vec![extract_common_fields(json, "L")],
        )),
        tokio::spawn(process_entity(
            Entity {
                dir_name: "Type",
                csv_name: "types",
                headers: vec!["id", "name", "name_en", "name_romaji", "name_alt"],
            },
            move |json| vec![extract_common_fields(json, "T")],
        )),
        // Special entities with custom transformations
        tokio::spawn(process_entity(
            Entity {
                dir_name: "Convention",
                csv_name: "conventions",
                headers: vec![
                    "id",
                    "name",
                    "name_en",
                    "name_romaji",
                    "name_alt",
                    "start_date",
                    "end_date",
                ],
            },
            move |json| {
                let mut record = extract_common_fields(json, "C");
                record.extend([get_str(json, "DATE_START"), get_str(json, "DATE_END")]);
                vec![record]
            },
        )),
        tokio::spawn(process_entity(
            Entity {
                dir_name: "Book",
                csv_name: "books",
                headers: vec![
                    "id",
                    "name",
                    "name_en",
                    "name_romaji",
                    "name_alt",
                    "author_id",
                    "convention_id",
                    "circle_id",
                    "genre_id",
                    "magazine_id",
                    "type_id",
                    "imprint_id",
                    "publisher_id",
                    "release_date",
                    "pages",
                    "language",
                    "is_adult",
                    "is_anthology",
                    "is_copybook",
                    "is_magazine",
                    "description",
                    "isbn",
                ],
            },
            move |json| {
                let links = extract_links(json);
                let mut record = extract_common_fields(json, "B");

                // Add all book-specific fields
                record.extend([
                    filter_links(&links, "A"),
                    filter_links(&links, "N"),
                    filter_links(&links, "C"),
                    filter_links(&links, "G"),
                    filter_links(&links, "O"),
                    filter_links(&links, "T"),
                    filter_links(&links, "I"),
                    filter_links(&links, "L"),
                    get_str(json, "DATE_RELEASED"),
                    get_str(json, "DATA_PAGES"),
                    get_str(json, "DATA_LANGUAGE"),
                    get_str(json, "DATA_AGE"),
                    get_str(json, "DATA_ANTHOLOGY"),
                    get_str(json, "DATA_COPYSHI"),
                    get_str(json, "DATA_MAGAZINE"),
                    get_str(json, "DATA_INFO"),
                    get_str(json, "DATA_ISBN"),
                ]);
                vec![record]
            },
        )),
        // Relationships - now using the updated transform function
        tokio::spawn(process_entity(
            Entity {
                dir_name: "Character",
                csv_name: "character_tags",
                headers: vec!["character_id", "tag_id"],
            },
            move |json| relationship_transform(json, "H", "K"),
        )),
        tokio::spawn(process_entity(
            Entity {
                dir_name: "Parody",
                csv_name: "parody_characters",
                headers: vec!["parody_id", "character_id"],
            },
            move |json| relationship_transform(json, "P", "H"),
        )),
        tokio::spawn(process_entity(
            Entity {
                dir_name: "Parody",
                csv_name: "parody_tags",
                headers: vec!["parody_id", "tag_id"],
            },
            move |json| relationship_transform(json, "P", "K"),
        )),
        tokio::spawn(process_entity(
            Entity {
                dir_name: "Book",
                csv_name: "book_authors",
                headers: vec!["book_id", "author_id"],
            },
            move |json| relationship_transform(json, "B", "A"),
        )),
        tokio::spawn(process_entity(
            Entity {
                dir_name: "Book",
                csv_name: "book_characters",
                headers: vec!["book_id", "character_id"],
            },
            move |json| relationship_transform(json, "B", "H"),
        )),
        tokio::spawn(process_entity(
            Entity {
                dir_name: "Book",
                csv_name: "book_tags",
                headers: vec!["book_id", "tag_id"],
            },
            move |json| relationship_transform(json, "B", "K"),
        )),
        tokio::spawn(process_entity(
            Entity {
                dir_name: "Book",
                csv_name: "book_parodies",
                headers: vec!["book_id", "parody_id"],
            },
            move |json| relationship_transform(json, "B", "P"),
        )),
    ];

    // Wait for all tasks to complete
    for task in future::join_all(tasks).await {
        if let Err(e) = task {
            eprintln!("Task failed: {:?}", e);
        }
    }

    println!("Processing completed.");
}
