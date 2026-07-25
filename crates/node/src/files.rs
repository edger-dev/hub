//! A reference connector: read-only file access under a fixed root (§1).
//!
//! This exists to prove the connector path end to end — a real capability with a
//! real payload codec the hub never sees inside. It is deliberately **read-only
//! and rooted**: every request path is resolved against the root and refused if
//! it escapes, so the connector cannot be talked out of its directory.

// implements: d78fb95fddc31aa42bc927da1eaed44292529b3671d1ef9ef58ec8cd0858e51c@d78fb95fddc31aa42bc927da1eaed44292529b3671d1ef9ef58ec8cd0858e51c

use std::path::{Component, Path, PathBuf};

use facet::Facet;
use hub_protocol::{Connector as ConnectorDescriptor, ConnectorKind, NodeError};

use crate::connector::Connector;

/// Ask for a directory listing or a file's contents.
#[derive(Facet, Debug, Clone, PartialEq, Eq)]
pub struct PathRequest {
    /// Path relative to the connector's root. `.` is the root itself.
    pub path: String,
}

/// One entry in a listing.
#[derive(Facet, Debug, Clone, PartialEq, Eq)]
pub struct DirEntry {
    pub name: String,
    pub is_dir: bool,
}

/// The answer to `list`.
#[derive(Facet, Debug, Clone, PartialEq, Eq)]
pub struct Listing {
    pub entries: Vec<DirEntry>,
}

/// The answer to `read`.
#[derive(Facet, Debug, Clone, PartialEq, Eq)]
pub struct FileContents {
    pub contents: String,
}

/// Read-only file access beneath [`FilesConnector::root`].
pub struct FilesConnector {
    id: String,
    root: PathBuf,
}

impl FilesConnector {
    /// Serve `root` under the connector id `id`.
    pub fn new(id: impl Into<String>, root: impl Into<PathBuf>) -> Self {
        Self {
            id: id.into(),
            root: root.into(),
        }
    }

    /// The directory this connector is confined to.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Resolve a request path against the root, refusing anything that escapes.
    ///
    /// Traversal is rejected *lexically* (`..` and absolute paths are refused
    /// outright) and then again after canonicalisation, which also catches a
    /// symlink pointing outside the root.
    fn resolve(&self, requested: &str) -> Result<PathBuf, NodeError> {
        let path = Path::new(requested);
        for component in path.components() {
            match component {
                Component::Normal(_) | Component::CurDir => {}
                // `..`, `/`, and Windows prefixes could all leave the root.
                _ => {
                    return Err(NodeError::Rejected(
                        "path escapes the connector root".to_string(),
                    ));
                }
            }
        }

        let joined = self.root.join(path);
        let canonical = joined
            .canonicalize()
            .map_err(|e| NodeError::Rejected(format!("cannot resolve path: {e}")))?;
        let root = self
            .root
            .canonicalize()
            .map_err(|e| NodeError::Rejected(format!("cannot resolve root: {e}")))?;
        if !canonical.starts_with(&root) {
            return Err(NodeError::Rejected(
                "path escapes the connector root".to_string(),
            ));
        }
        Ok(canonical)
    }

    fn list(&self, request: &PathRequest) -> Result<Listing, NodeError> {
        let dir = self.resolve(&request.path)?;
        let read = std::fs::read_dir(&dir)
            .map_err(|e| NodeError::Rejected(format!("cannot list directory: {e}")))?;
        let mut entries: Vec<DirEntry> = read
            .flatten()
            .map(|entry| DirEntry {
                name: entry.file_name().to_string_lossy().into_owned(),
                is_dir: entry.file_type().map(|t| t.is_dir()).unwrap_or(false),
            })
            .collect();
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(Listing { entries })
    }

    fn read(&self, request: &PathRequest) -> Result<FileContents, NodeError> {
        let file = self.resolve(&request.path)?;
        let contents = std::fs::read_to_string(&file)
            .map_err(|e| NodeError::Rejected(format!("cannot read file: {e}")))?;
        Ok(FileContents { contents })
    }
}

/// Decode a request payload. The codec is this connector's business (§1).
fn decode(payload: &[u8]) -> Result<PathRequest, NodeError> {
    let text = std::str::from_utf8(payload)
        .map_err(|e| NodeError::Rejected(format!("payload is not utf-8: {e}")))?;
    facet_json::from_str(text).map_err(|e| NodeError::Rejected(format!("bad request: {e}")))
}

/// Encode a response payload.
fn encode<'a, T: Facet<'a>>(value: &T) -> Result<Vec<u8>, NodeError> {
    facet_json::to_string(value)
        .map(String::into_bytes)
        .map_err(|e| NodeError::Rejected(format!("cannot encode reply: {e}")))
}

impl Connector for FilesConnector {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor {
            id: self.id.clone(),
            kind: ConnectorKind::Files,
        }
    }

    fn call(&self, method: &str, payload: &[u8]) -> Result<Vec<u8>, NodeError> {
        match method {
            "list" => encode(&self.list(&decode(payload)?)?),
            "read" => encode(&self.read(&decode(payload)?)?),
            other => Err(NodeError::Rejected(format!("unknown method `{other}`"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (tempdir::TempDir, FilesConnector) {
        let dir = tempdir::TempDir::new("files-connector").expect("tempdir");
        std::fs::write(dir.path().join("hello.txt"), "hi there").expect("write");
        std::fs::create_dir(dir.path().join("sub")).expect("mkdir");
        let connector = FilesConnector::new("files-0", dir.path());
        (dir, connector)
    }

    fn request(path: &str) -> Vec<u8> {
        facet_json::to_string(&PathRequest {
            path: path.to_string(),
        })
        .expect("encode")
        .into_bytes()
    }

    #[test]
    fn reads_a_file_under_the_root() {
        let (_dir, c) = fixture();
        let reply = c.call("read", &request("hello.txt")).expect("read");
        let contents: FileContents =
            facet_json::from_str(std::str::from_utf8(&reply).unwrap()).expect("decode");
        assert_eq!(contents.contents, "hi there");
    }

    #[test]
    fn lists_the_root() {
        let (_dir, c) = fixture();
        let reply = c.call("list", &request(".")).expect("list");
        let listing: Listing =
            facet_json::from_str(std::str::from_utf8(&reply).unwrap()).expect("decode");
        let names: Vec<&str> = listing.entries.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, vec!["hello.txt", "sub"]);
        assert!(listing.entries[1].is_dir);
    }

    #[test]
    fn refuses_parent_traversal() {
        let (_dir, c) = fixture();
        let err = c.call("read", &request("../secret")).unwrap_err();
        assert!(
            matches!(&err, NodeError::Rejected(m) if m.contains("escapes")),
            "got {err:?}"
        );
    }

    #[test]
    fn refuses_an_absolute_path() {
        let (_dir, c) = fixture();
        let err = c.call("read", &request("/etc/hostname")).unwrap_err();
        assert!(
            matches!(&err, NodeError::Rejected(m) if m.contains("escapes")),
            "got {err:?}"
        );
    }

    #[test]
    fn refuses_a_symlink_out_of_the_root() {
        let (dir, c) = fixture();
        let link = dir.path().join("escape");
        #[cfg(unix)]
        std::os::unix::fs::symlink("/etc", &link).expect("symlink");
        let err = c.call("list", &request("escape")).unwrap_err();
        assert!(
            matches!(&err, NodeError::Rejected(m) if m.contains("escapes")),
            "got {err:?}"
        );
    }

    #[test]
    fn refuses_an_unknown_method() {
        let (_dir, c) = fixture();
        let err = c.call("delete", &request("hello.txt")).unwrap_err();
        assert!(matches!(err, NodeError::Rejected(_)));
    }

    #[test]
    fn advertises_itself_as_a_files_connector() {
        let (_dir, c) = fixture();
        let d = c.descriptor();
        assert_eq!(d.id, "files-0");
        assert_eq!(d.kind, ConnectorKind::Files);
    }
}
