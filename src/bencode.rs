use std::str;

use thiserror::Error;

const MAX_NESTING_DEPTH: usize = 128;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Value<'a> {
    Integer(i128),
    Bytes(&'a [u8]),
    List(Vec<Value<'a>>),
    Dictionary(Vec<(&'a [u8], Value<'a>)>),
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum DecodeError {
    #[error("unexpected end of bencode input at byte {offset}")]
    UnexpectedEnd { offset: usize },
    #[error("invalid bencode token 0x{token:02x} at byte {offset}")]
    InvalidToken { offset: usize, token: u8 },
    #[error("invalid bencode integer at byte {offset}")]
    InvalidInteger { offset: usize },
    #[error("invalid byte string length at byte {offset}")]
    InvalidByteStringLength { offset: usize },
    #[error("byte string at byte {offset} exceeds the remaining input")]
    TruncatedByteString { offset: usize },
    #[error("dictionary key at byte {offset} is not a byte string")]
    InvalidDictionaryKey { offset: usize },
    #[error("bencode nesting exceeds {MAX_NESTING_DEPTH} levels at byte {offset}")]
    NestingTooDeep { offset: usize },
    #[error("trailing data begins at byte {offset}")]
    TrailingData { offset: usize },
}

pub fn decode(input: &[u8]) -> Result<Value<'_>, DecodeError> {
    let mut parser = Parser { input, offset: 0 };
    let value = parser.parse_value(0)?;
    if parser.offset != input.len() {
        return Err(DecodeError::TrailingData {
            offset: parser.offset,
        });
    }
    Ok(value)
}

struct Parser<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> Parser<'a> {
    fn parse_value(&mut self, depth: usize) -> Result<Value<'a>, DecodeError> {
        if depth >= MAX_NESTING_DEPTH {
            return Err(DecodeError::NestingTooDeep {
                offset: self.offset,
            });
        }
        let token = self.peek()?;
        match token {
            b'i' => self.parse_integer(),
            b'l' => self.parse_list(depth + 1),
            b'd' => self.parse_dictionary(depth + 1),
            b'0'..=b'9' => self.parse_bytes().map(Value::Bytes),
            _ => Err(DecodeError::InvalidToken {
                offset: self.offset,
                token,
            }),
        }
    }

    fn parse_integer(&mut self) -> Result<Value<'a>, DecodeError> {
        let start = self.offset;
        self.offset += 1;
        let value_start = self.offset;
        while self.peek()? != b'e' {
            self.offset += 1;
        }
        let value = &self.input[value_start..self.offset];
        self.offset += 1;
        let digits = value.strip_prefix(b"-").unwrap_or(value);
        let invalid_encoding = value.is_empty()
            || digits.is_empty()
            || !digits.iter().all(u8::is_ascii_digit)
            || value == b"-0"
            || (value.len() > 1 && value[0] == b'0')
            || (value.len() > 2 && value.starts_with(b"-0"));
        if invalid_encoding {
            return Err(DecodeError::InvalidInteger { offset: start });
        }
        let value = str::from_utf8(value)
            .ok()
            .and_then(|value| value.parse::<i128>().ok())
            .ok_or(DecodeError::InvalidInteger { offset: start })?;
        Ok(Value::Integer(value))
    }

    fn parse_bytes(&mut self) -> Result<&'a [u8], DecodeError> {
        let start = self.offset;
        while self.peek()? != b':' {
            if !self.input[self.offset].is_ascii_digit() {
                return Err(DecodeError::InvalidByteStringLength { offset: start });
            }
            self.offset += 1;
        }
        let length_bytes = &self.input[start..self.offset];
        self.offset += 1;
        if length_bytes.is_empty()
            || (length_bytes.len() > 1 && length_bytes.first() == Some(&b'0'))
        {
            return Err(DecodeError::InvalidByteStringLength { offset: start });
        }
        let length = str::from_utf8(length_bytes)
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .ok_or(DecodeError::InvalidByteStringLength { offset: start })?;
        let end = self
            .offset
            .checked_add(length)
            .filter(|end| *end <= self.input.len())
            .ok_or(DecodeError::TruncatedByteString { offset: start })?;
        let value = &self.input[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn parse_list(&mut self, depth: usize) -> Result<Value<'a>, DecodeError> {
        self.offset += 1;
        let mut values = Vec::new();
        loop {
            if self.peek()? == b'e' {
                self.offset += 1;
                return Ok(Value::List(values));
            }
            values.push(self.parse_value(depth)?);
        }
    }

    fn parse_dictionary(&mut self, depth: usize) -> Result<Value<'a>, DecodeError> {
        self.offset += 1;
        let mut entries = Vec::new();
        loop {
            if self.peek()? == b'e' {
                self.offset += 1;
                return Ok(Value::Dictionary(entries));
            }
            let key_offset = self.offset;
            if !self.peek()?.is_ascii_digit() {
                return Err(DecodeError::InvalidDictionaryKey { offset: key_offset });
            }
            let key = self.parse_bytes()?;
            let value = self.parse_value(depth)?;
            entries.push((key, value));
        }
    }

    fn peek(&self) -> Result<u8, DecodeError> {
        self.input
            .get(self.offset)
            .copied()
            .ok_or(DecodeError::UnexpectedEnd {
                offset: self.offset,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_nested_bencode_when_decoded_then_structure_is_preserved() {
        let decoded = decode(b"d3:cow3:moo4:spamli1ei2eee").expect("value should decode");

        assert_eq!(
            decoded,
            Value::Dictionary(vec![
                (b"cow", Value::Bytes(b"moo")),
                (
                    b"spam",
                    Value::List(vec![Value::Integer(1), Value::Integer(2)])
                )
            ])
        );
    }

    #[test]
    fn given_binary_bytes_when_decoded_then_no_utf8_conversion_is_required() {
        assert_eq!(
            decode(b"4:\x00\xff\x01\x80").expect("binary bytes should decode"),
            Value::Bytes(&[0, 255, 1, 128])
        );
    }

    #[test]
    fn given_noncanonical_or_truncated_values_when_decoded_then_errors_are_returned() {
        for input in [
            b"i03e".as_slice(),
            b"i-0e",
            b"03:abc",
            b"4:abc",
            b"dli1eee",
            b"i1eextra",
        ] {
            assert!(decode(input).is_err(), "{input:?} should be rejected");
        }
    }

    #[test]
    fn given_excessive_nesting_when_decoded_then_depth_is_bounded() {
        let mut input = vec![b'l'; MAX_NESTING_DEPTH + 1];
        input.extend(std::iter::repeat_n(b'e', MAX_NESTING_DEPTH + 1));

        assert!(matches!(
            decode(&input),
            Err(DecodeError::NestingTooDeep { .. })
        ));
    }
}
