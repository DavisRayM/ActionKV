use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc::{Crc, CRC_32_CKSUM};
use serde::{Deserialize, Serialize};

pub type ByteStr = [u8];
pub type ByteString = Vec<u8>;

#[derive(Serialize, Deserialize)]
pub struct KeyValuePair {
    pub key: ByteString,
    pub value: ByteString,
}

#[derive(Debug)]
pub struct ActionKV {
    f: File,
    pub index: HashMap<ByteString, u64>,
}

static CRC32: crc::Crc<u32> = Crc::<u32>::new(&CRC_32_CKSUM);

impl ActionKV {
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(path)?;
        let index = HashMap::new();
        Ok(Self { f, index })
    }

    pub fn load(&mut self) -> std::io::Result<()> {
        let mut f = BufReader::new(&mut self.f);

        loop {
            let pos = f.seek(SeekFrom::Current(0))?;

            let maybe_kv = ActionKV::process_record(&mut f);
            let kv = match maybe_kv {
                Ok(kv) => kv,
                Err(err) => match err.kind() {
                    std::io::ErrorKind::UnexpectedEof => {
                        break;
                    }
                    _ => return Err(err),
                },
            };

            self.index.insert(kv.key, pos);
        }

        Ok(())
    }

    pub fn insert(&mut self, key: &ByteStr, value: &ByteStr) -> std::io::Result<()> {
        let pos = self.insert_but_ignore_index(key, value)?;

        self.index.insert(key.to_vec(), pos);
        Ok(())
    }

    /// Inserts data into the log structured store without updating the KV internal index
    ///
    /// Inserted data is added in the format <checksum><key_len><value_len><value>; This is to
    /// ensure resiliency of the stored data.
    pub fn insert_but_ignore_index(
        &mut self,
        key: &ByteStr,
        value: &ByteStr,
    ) -> std::io::Result<u64> {
        let mut f = BufWriter::new(&mut self.f);

        let key_len = key.len();
        let value_len = value.len();
        let mut tmp = ByteString::with_capacity(key_len + value_len);

        for byte in key {
            tmp.push(*byte);
        }
        for byte in value {
            tmp.push(*byte);
        }

        let checksum = CRC32.checksum(&tmp);

        let next_byte = SeekFrom::End(0);
        let current_position = f.seek(SeekFrom::Current(0))?;
        f.seek(next_byte)?;
        f.write_u32::<LittleEndian>(checksum)?;
        f.write_u32::<LittleEndian>(key_len as u32)?;
        f.write_u32::<LittleEndian>(value_len as u32)?;
        f.write_all(&tmp)?;

        Ok(current_position)
    }

    pub fn get(&mut self, key: &ByteStr) -> std::io::Result<Option<ByteString>> {
        let pos = match self.index.get(key) {
            None => return Ok(None),
            Some(pos) => *pos,
        };

        let kv = self.get_at(pos)?;

        Ok(Some(kv.value))
    }

    pub fn get_at(&mut self, position: u64) -> std::io::Result<KeyValuePair> {
        let mut f = BufReader::new(&mut self.f);
        f.seek(SeekFrom::Start(position))?;
        let kv = ActionKV::process_record(&mut f)?;

        Ok(kv)
    }

    pub fn find(&mut self, target: &ByteStr) -> std::io::Result<Option<(u64, ByteString)>> {
        let mut f = BufReader::new(&mut self.f);

        let mut found: Option<(u64, ByteString)> = None;
        loop {
            let pos = f.seek(SeekFrom::Current(0))?;

            let maybe_kv = ActionKV::process_record(&mut f);
            let kv = match maybe_kv {
                Ok(kv) => kv,
                Err(err) => match err.kind() {
                    std::io::ErrorKind::UnexpectedEof => {
                        break;
                    }
                    _ => return Err(err),
                },
            };

            if kv.key == target {
                found = Some((pos, kv.value));
            }
        }

        Ok(found)
    }

    #[inline]
    pub fn update(&mut self, key: &ByteStr, value: &ByteStr) -> std::io::Result<()> {
        self.insert(key, value)
    }

    #[inline]
    pub fn delete(&mut self, key: &ByteStr) -> std::io::Result<()> {
        self.insert(key, b"")
    }

    fn process_record<R: Read>(f: &mut R) -> std::io::Result<KeyValuePair> {
        let saved_checksum = f.read_u32::<LittleEndian>()?;
        let saved_key_len = f.read_u32::<LittleEndian>()?;
        let saved_value_len = f.read_u32::<LittleEndian>()?;
        let data_len = saved_key_len + saved_value_len;

        let mut data = ByteString::with_capacity(data_len as usize);
        {
            f.by_ref().take(data_len as u64).read_to_end(&mut data)?;
        };
        debug_assert_eq!(data.len(), data_len as usize);

        let checksum = CRC32.checksum(&data);
        if checksum != saved_checksum {
            panic!(
                "data corruption encountered: ({:08x} != {:08x})",
                checksum, saved_checksum
            );
        }

        let value = data.split_off(saved_key_len as usize);
        let key = data;

        Ok(KeyValuePair { key, value })
    }
}
