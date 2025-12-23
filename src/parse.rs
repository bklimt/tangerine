struct TokenSlice<'a> {
    token: &'a str,
    line: usize,
    column: usize,
    offset: usize,
}

impl<'a> TokenSlice<'a> {
    fn to_token(&self) -> Token {
        return Token {
            token: self.token.to_string(),
            line: self.line,
            column: self.column,
            offset: self.offset,
        };
    }
}

struct Token {
    token: String,
    line: usize,
    column: usize,
    offset: usize,
}

trait Indexer {
    fn process_token(&mut self, token: &TokenSlice);
}

fn parse_text(text: &str, indexer: &mut impl Indexer) {
    let mut start;
    let mut end;
    let mut line = 0;
    let mut column = 0;
    let mut word_column = 0;
    let mut chars_indices = text.char_indices();
    loop {
        // Skip to the next alphanumeric character.
        loop {
            let Some((i, c)) = chars_indices.next() else {
                return;
            };
            word_column = column;
            column += 1;
            if c.is_alphanumeric() {
                start = i;
                break;
            }
            if c == '\n' {
                line += 1;
                column = 0;
            }
        }

        // Now find the end of the word.
        let mut ended_with_newline = false;
        loop {
            let Some((i, c)) = chars_indices.next() else {
                end = text.len();
                break;
            };
            column += 1;
            if !c.is_alphanumeric() {
                end = i;
                if c == '\n' {
                    ended_with_newline = true;
                    column = 0;
                }
                break;
            }
        }

        // Then process the word.
        indexer.process_token(&TokenSlice {
            token: &text[start..end],
            line,
            column: word_column,
            offset: start,
        });

        if ended_with_newline {
            line += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestIndex {
        tokens: Vec<Token>,
    }

    impl TestIndex {
        fn new() -> Self {
            return TestIndex { tokens: Vec::new() };
        }
    }

    impl Indexer for TestIndex {
        fn process_token(&mut self, token: &TokenSlice) {
            self.tokens.push(token.to_token());
        }
    }

    #[test]
    fn test_parse_text() {
        let mut index = TestIndex::new();

        parse_text("foo", &mut index);

        assert_eq!(1, index.tokens.len());

        let token = index.tokens.get(0).unwrap();
        assert_eq!("foo", token.token);
        assert_eq!(0, token.line);
        assert_eq!(0, token.column);
        assert_eq!(0, token.offset);
    }

    #[test]
    fn test_parse_text_multiple_tokens() {
        let mut index = TestIndex::new();

        parse_text("foo bar", &mut index);

        assert_eq!(2, index.tokens.len());

        let token = index.tokens.get(0).unwrap();
        assert_eq!("foo", token.token);
        assert_eq!(0, token.line);
        assert_eq!(0, token.column);
        assert_eq!(0, token.offset);

        let token = index.tokens.get(1).unwrap();
        assert_eq!("bar", token.token);
        assert_eq!(0, token.line);
        assert_eq!(4, token.column);
        assert_eq!(4, token.offset);
    }

    #[test]
    fn test_parse_text_space_before() {
        let mut index = TestIndex::new();

        parse_text("  foo bar", &mut index);

        assert_eq!(2, index.tokens.len());

        let token = index.tokens.get(0).unwrap();
        assert_eq!("foo", token.token);
        assert_eq!(0, token.line);
        assert_eq!(2, token.column);
        assert_eq!(2, token.offset);

        let token = index.tokens.get(1).unwrap();
        assert_eq!("bar", token.token);
        assert_eq!(0, token.line);
        assert_eq!(6, token.column);
        assert_eq!(6, token.offset);
    }

    #[test]
    fn test_parse_text_space_after() {
        let mut index = TestIndex::new();

        parse_text("foo bar  ", &mut index);

        assert_eq!(2, index.tokens.len());

        let token = index.tokens.get(0).unwrap();
        assert_eq!("foo", token.token);
        assert_eq!(0, token.line);
        assert_eq!(0, token.column);
        assert_eq!(0, token.offset);

        let token = index.tokens.get(1).unwrap();
        assert_eq!("bar", token.token);
        assert_eq!(0, token.line);
        assert_eq!(4, token.column);
        assert_eq!(4, token.offset);
    }

    #[test]
    fn test_parse_text_with_numbers() {
        let mut index = TestIndex::new();

        parse_text("foo123bar", &mut index);

        assert_eq!(1, index.tokens.len());

        let token = index.tokens.get(0).unwrap();
        assert_eq!("foo123bar", token.token);
        assert_eq!(0, token.line);
        assert_eq!(0, token.column);
        assert_eq!(0, token.offset);
    }

    #[test]
    fn test_parse_text_with_punctuation() {
        let mut index = TestIndex::new();

        parse_text("foo.bar", &mut index);

        assert_eq!(2, index.tokens.len());

        let token = index.tokens.get(0).unwrap();
        assert_eq!("foo", token.token);
        assert_eq!(0, token.line);
        assert_eq!(0, token.column);
        assert_eq!(0, token.offset);

        let token = index.tokens.get(1).unwrap();
        assert_eq!("bar", token.token);
        assert_eq!(0, token.line);
        assert_eq!(4, token.column);
        assert_eq!(4, token.offset);
    }

    #[test]
    fn test_parse_text_with_camelcase() {
        let mut index = TestIndex::new();

        parse_text("FooBar123", &mut index);

        assert_eq!(1, index.tokens.len());

        let token = index.tokens.get(0).unwrap();
        assert_eq!("FooBar123", token.token);
        assert_eq!(0, token.line);
        assert_eq!(0, token.column);
        assert_eq!(0, token.offset);
    }

    #[test]
    fn test_parse_text_with_3byte_char() {
        let mut index = TestIndex::new();

        parse_text("福 福foo福bar福", &mut index);

        assert_eq!(2, index.tokens.len());

        let token = index.tokens.get(0).unwrap();
        assert_eq!("福", token.token);
        assert_eq!(0, token.line);
        assert_eq!(0, token.column);
        assert_eq!(0, token.offset);

        let token = index.tokens.get(1).unwrap();
        assert_eq!("福foo福bar福", token.token);
        assert_eq!(0, token.line);
        assert_eq!(2, token.column);
        assert_eq!(4, token.offset);
    }

    #[test]
    fn test_parse_text_with_4byte_char() {
        let mut index = TestIndex::new();

        parse_text("💜 💜foo💜bar💜", &mut index);

        assert_eq!(2, index.tokens.len());

        let token = index.tokens.get(0).unwrap();
        assert_eq!("foo", token.token);
        assert_eq!(0, token.line);
        assert_eq!(3, token.column);
        assert_eq!(9, token.offset);

        let token = index.tokens.get(1).unwrap();
        assert_eq!("bar", token.token);
        assert_eq!(0, token.line);
        assert_eq!(7, token.column);
        assert_eq!(16, token.offset);
    }

    #[test]
    fn test_parse_text_with_line_and_column() {
        let mut index = TestIndex::new();

        parse_text("foo\n  \n  bar", &mut index);

        assert_eq!(2, index.tokens.len());

        let token = index.tokens.get(0).unwrap();
        assert_eq!("foo", token.token);
        assert_eq!(0, token.line);
        assert_eq!(0, token.column);
        assert_eq!(0, token.offset);

        let token = index.tokens.get(1).unwrap();
        assert_eq!("bar", token.token);
        assert_eq!(2, token.line);
        assert_eq!(2, token.column);
        assert_eq!(9, token.offset);
    }
}
