use anyhow::Result;
use futures::future;
use glob::glob;
use indicatif::{MultiProgress, ParallelProgressIterator, ProgressBar, ProgressIterator};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use serde_json::Value;
use std::{path::PathBuf, sync::LazyLock};

static DATA_DIR: LazyLock<PathBuf> = LazyLock::new(|| PathBuf::from("data/doujinshi.org"));
static MULTI_PROGRESS: LazyLock<MultiProgress> = LazyLock::new(MultiProgress::new);

fn progress_bar() -> ProgressBar {
    MULTI_PROGRESS
        .add(ProgressBar::new_spinner().with_style(indicatif::ProgressStyle::default_spinner()))
}

// Entity and relationship types, with their properties
enum Type {
    // Entities
    Author,
    Character,
    Tag,
    Convention,
    Genre,
    Series,
    Imprint,
    Parody,
    Publisher,
    Type,
    Book,
    // Relationships
    CharacterTag,
    ParodyCharacter,
    ParodyTag,
    BookAuthor,
    BookCharacter,
    BookTag,
    BookParody,
}

impl Type {
    fn dir_name(&self) -> &'static str {
        match self {
            Type::Author => "Author",
            Type::Character => "Character",
            Type::Tag => "Content",
            Type::Convention => "Convention",
            Type::Genre => "Genre",
            Type::Series => "Collections",
            Type::Imprint => "Imprint",
            Type::Parody => "Parody",
            Type::Publisher | Type::Book => "Book",
            Type::Type => "Type",
            _ => self.source().dir_name(), // For relationships, use source entity's dir
        }
    }

    fn id_prefix(&self) -> &'static str {
        match self {
            Type::Author => "A",
            Type::Character => "H",
            Type::Tag => "K",
            Type::Convention => "C",
            Type::Genre => "G",
            Type::Series => "O",
            Type::Imprint => "I",
            Type::Parody => "P",
            Type::Publisher => "B",
            Type::Type => "T",
            Type::Book => "B",
            _ => "", // Not used directly for relationships
        }
    }

    fn csv_name(&self) -> String {
        match self {
            Type::Author => "authors",
            Type::Character => "characters",
            Type::Tag => "tags",
            Type::Convention => "conventions",
            Type::Genre => "genres",
            Type::Series => "series",
            Type::Imprint => "imprints",
            Type::Parody => "parodies",
            Type::Publisher => "publishers",
            Type::Type => "types",
            Type::Book => "books",
            Type::CharacterTag => "character_tags",
            Type::ParodyCharacter => "parody_characters",
            Type::ParodyTag => "parody_tags",
            Type::BookAuthor => "book_authors",
            Type::BookCharacter => "book_characters",
            Type::BookTag => "book_tags",
            Type::BookParody => "book_parodies",
        }
        .to_string()
    }

    fn headers(&self) -> Vec<&'static str> {
        match self {
            Type::Convention => vec![
                "id",
                "name",
                "name_en",
                "name_romaji",
                "name_alt",
                "start_date",
                "end_date",
            ],
            Type::Book => vec![
                "id",
                "name",
                "name_en",
                "name_romaji",
                "name_alt",
                "author_id",
                "convention_id",
                "circle_id",
                "genre_id",
                "series_id",
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
            Type::Author
            | Type::Character
            | Type::Tag
            | Type::Genre
            | Type::Series
            | Type::Imprint
            | Type::Parody
            | Type::Publisher
            | Type::Type => vec!["id", "name", "name_en", "name_romaji", "name_alt"],
            // Relationships
            Type::CharacterTag => vec!["character_id", "tag_id"],
            Type::ParodyCharacter => vec!["parody_id", "character_id"],
            Type::ParodyTag => vec!["parody_id", "tag_id"],
            Type::BookAuthor => vec!["book_id", "author_id"],
            Type::BookCharacter => vec!["book_id", "character_id"],
            Type::BookTag => vec!["book_id", "tag_id"],
            Type::BookParody => vec!["book_id", "parody_id"],
        }
    }

    fn is_relationship(&self) -> bool {
        matches!(
            self,
            Type::CharacterTag
                | Type::ParodyCharacter
                | Type::ParodyTag
                | Type::BookAuthor
                | Type::BookCharacter
                | Type::BookTag
                | Type::BookParody
        )
    }

    fn source(&self) -> Type {
        match self {
            Type::CharacterTag => Type::Character,
            Type::ParodyCharacter | Type::ParodyTag => Type::Parody,
            Type::BookAuthor | Type::BookCharacter | Type::BookTag | Type::BookParody => Type::Book,
            _ => panic!("Not a relationship type"),
        }
    }

    fn target(&self) -> Type {
        match self {
            Type::CharacterTag | Type::ParodyTag | Type::BookTag => Type::Tag,
            Type::ParodyCharacter | Type::BookCharacter => Type::Character,
            Type::BookAuthor => Type::Author,
            Type::BookParody => Type::Parody,
            _ => panic!("Not a relationship type"),
        }
    }

    // Get the transform function for this type
    fn transform(&self) -> Box<dyn Fn(&Value) -> Vec<String> + Send + Sync> {
        if self.is_relationship() {
            self.relationship_transform()
        } else {
            self.entity_transform()
        }
    }

    // Transform function for entity types
    fn entity_transform(&self) -> Box<dyn Fn(&Value) -> Vec<String> + Send + Sync> {
        let id_prefix = self.id_prefix().to_string();

        match self {
            Type::Convention => Box::new(move |json| {
                let mut record = extract_common_fields(json, &id_prefix);
                record.extend([get_str(json, "DATE_START"), get_str(json, "DATE_END")]);
                record
            }),
            Type::Book => Box::new(move |json| {
                let links = extract_links(json);
                let mut record = extract_common_fields(json, &id_prefix);

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
                record
            }),
            _ => Box::new(move |json| extract_common_fields(json, &id_prefix)),
        }
    }

    // Transform function for relationship types
    fn relationship_transform(&self) -> Box<dyn Fn(&Value) -> Vec<String> + Send + Sync> {
        let source = self.source();
        let target = self.target();
        let source_prefix = source.id_prefix().to_string();
        let target_prefix = target.id_prefix().to_string();

        Box::new(move |json| {
            let links = extract_links(json);
            let related_items = links
                .into_iter()
                .filter(|tag| tag.starts_with(&target_prefix))
                .map(|tag| tag.replace(&target_prefix, ""))
                .collect::<Vec<_>>();

            if related_items.is_empty() {
                return vec![];
            }

            vec![
                get_str(json, "@ID").replace(&source_prefix, ""),
                related_items.join(","),
            ]
        })
    }
}

// Helper to safely extract string values from JSON
fn get_str(json: &Value, key: &str) -> String {
    json[key].as_str().unwrap_or_default().to_string()
}

// Helper functions for data extraction
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

// Core processing function
async fn process(typ: Type) -> Result<()> {
    let pattern = DATA_DIR
        .join(typ.dir_name())
        .join("*.json")
        .to_string_lossy()
        .to_string();
    let output_file = DATA_DIR
        .join(format!("{}.csv", typ.csv_name()))
        .to_string_lossy()
        .to_string();
    let headers = typ.headers();
    let transform = typ.transform();

    let mut writer = csv::Writer::from_path(output_file)?;
    writer.write_record(&headers)?;

    glob(&pattern)?
        .filter_map(Result::ok)
        .collect::<Vec<_>>()
        .par_iter()
        .progress_with(
            progress_bar().with_message(format!("Processing files matching pattern: {}", pattern)),
        )
        .map(|file| {
            let json: Value = serde_json::from_reader(std::io::BufReader::new(
                std::fs::File::open(file).unwrap(),
            ))
            .unwrap();
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

#[tokio::main]
async fn main() {
    let types = vec![
        // Entities
        Type::Author,
        Type::Character,
        Type::Tag,
        Type::Convention,
        Type::Genre,
        Type::Series,
        Type::Imprint,
        Type::Parody,
        Type::Publisher,
        Type::Type,
        Type::Book,
        // Relationships
        Type::CharacterTag,
        Type::ParodyCharacter,
        Type::ParodyTag,
        Type::BookAuthor,
        Type::BookCharacter,
        Type::BookTag,
        Type::BookParody,
    ];

    future::join_all(types.into_iter().map(|typ| tokio::spawn(process(typ)))).await;
    println!("Processing completed.")
}
