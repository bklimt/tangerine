pub struct TokenSlice<'a> {
    pub token: &'a str,
    pub line: usize,
    pub column: usize,
    pub offset: usize,
    pub partial: bool,
}

pub trait TokenProcessor {
    fn process_token(&mut self, token: &TokenSlice);
}

fn split_token(token: &TokenSlice, indexer: &mut impl TokenProcessor) {
    #[derive(PartialEq)]
    enum TokenState {
        Start,
        Nocase,
        Uppercase,
        Lowercase,
        Digits,
        End,
    }

    let mut chars_indices = token.token.char_indices().peekable();

    let mut word_start = 0;
    let mut word_end;
    let mut next_state = TokenState::Start;
    // In order to undo one read, we peek, and only call next if this is true.
    let mut consume_next = false;
    loop {
        let previous_state = next_state;
        if matches!(previous_state, TokenState::End) {
            return;
        }

        if consume_next {
            chars_indices.next();
        }
        consume_next = true;
        if let Some((i, c)) = chars_indices.peek() {
            word_end = *i;
            if c.is_numeric() {
                next_state = TokenState::Digits;
            } else if c.is_lowercase() {
                next_state = TokenState::Lowercase;
            } else if c.is_uppercase() {
                next_state = TokenState::Uppercase;
            } else {
                next_state = TokenState::Nocase;
            }
        } else {
            word_end = token.token.len();
            next_state = TokenState::End;
        };

        // If the state didn't change, continue the word.
        if previous_state == next_state {
            continue;
        }
        // If this is the first character, this can't be the start of a new token.
        if matches!(previous_state, TokenState::Start) {
            continue;
        }
        // If this is the end and the whole token was one word, skip it.
        if matches!(next_state, TokenState::End) && word_start == 0 {
            continue;
        }
        // If we go from uppercase to lowercase and is one character, that's fine.
        if matches!(previous_state, TokenState::Uppercase)
            && matches!(next_state, TokenState::Lowercase)
        {
            // Well, if they are one uppercase character at least.
            if word_end - word_start == 1 {
                continue;
            }

            // Find the length of the last character in the token so far.
            let word = &token.token[word_start..word_end];
            let Some((last_char, _)) = word.char_indices().last() else {
                continue;
            };
            let last_char_len = word.len() - last_char;
            if last_char_len == word.len() {
                continue;
            }

            // If it was a longer uppercase sequence, emit all but the last byte.
            indexer.process_token(&TokenSlice {
                token: &token.token[word_start..word_end - last_char_len],
                line: token.line,
                column: token.column,
                offset: token.offset + word_start,
                partial: true,
            });
            word_start = word_end - last_char_len;

            // Rewind by one character.
            consume_next = false;
            continue;
        }

        // Emit the word.
        indexer.process_token(&TokenSlice {
            token: &token.token[word_start..word_end],
            line: token.line,
            column: token.column,
            offset: token.offset + word_start,
            partial: true,
        });
        word_start = word_end;
    }
}

pub fn parse_text(text: &str, indexer: &mut impl TokenProcessor) {
    let mut start;
    let mut end;
    let mut line = 0;
    let mut column = 0;
    let mut word_column;
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
        let token = TokenSlice {
            token: &text[start..end],
            line,
            column: word_column,
            offset: start,
            partial: false,
        };
        indexer.process_token(&token);
        split_token(&token, indexer);

        if ended_with_newline {
            line += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl<'a> TokenSlice<'a> {
        fn to_token(&self) -> Token {
            return Token {
                token: self.token.to_string(),
                line: self.line,
                column: self.column,
                offset: self.offset,
                partial: self.partial,
            };
        }
    }

    struct Token {
        token: String,
        line: usize,
        column: usize,
        offset: usize,
        partial: bool,
    }

    struct TestIndex {
        tokens: Vec<Token>,
    }

    impl TestIndex {
        fn new() -> Self {
            return TestIndex { tokens: Vec::new() };
        }
    }

    impl TokenProcessor for TestIndex {
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
        assert_eq!(false, token.partial);
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
        assert_eq!(false, token.partial);

        let token = index.tokens.get(1).unwrap();
        assert_eq!("bar", token.token);
        assert_eq!(0, token.line);
        assert_eq!(4, token.column);
        assert_eq!(4, token.offset);
        assert_eq!(false, token.partial);
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
        assert_eq!(false, token.partial);

        let token = index.tokens.get(1).unwrap();
        assert_eq!("bar", token.token);
        assert_eq!(0, token.line);
        assert_eq!(6, token.column);
        assert_eq!(6, token.offset);
        assert_eq!(false, token.partial);
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
        assert_eq!(false, token.partial);

        let token = index.tokens.get(1).unwrap();
        assert_eq!("bar", token.token);
        assert_eq!(0, token.line);
        assert_eq!(4, token.column);
        assert_eq!(4, token.offset);
        assert_eq!(false, token.partial);
    }

    #[test]
    fn test_parse_text_with_numbers() {
        let mut index = TestIndex::new();

        parse_text("foo123bar", &mut index);

        assert_eq!(4, index.tokens.len());

        let token = index.tokens.get(0).unwrap();
        assert_eq!("foo123bar", token.token);
        assert_eq!(0, token.line);
        assert_eq!(0, token.column);
        assert_eq!(0, token.offset);
        assert_eq!(false, token.partial);

        let token = index.tokens.get(1).unwrap();
        assert_eq!("foo", token.token);
        assert_eq!(0, token.line);
        assert_eq!(0, token.column);
        assert_eq!(0, token.offset);
        assert_eq!(true, token.partial);

        let token = index.tokens.get(2).unwrap();
        assert_eq!("123", token.token);
        assert_eq!(0, token.line);
        assert_eq!(0, token.column);
        assert_eq!(3, token.offset);
        assert_eq!(true, token.partial);

        let token = index.tokens.get(3).unwrap();
        assert_eq!("bar", token.token);
        assert_eq!(0, token.line);
        assert_eq!(0, token.column);
        assert_eq!(6, token.offset);
        assert_eq!(true, token.partial);
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
        assert_eq!(false, token.partial);

        let token = index.tokens.get(1).unwrap();
        assert_eq!("bar", token.token);
        assert_eq!(0, token.line);
        assert_eq!(4, token.column);
        assert_eq!(4, token.offset);
        assert_eq!(false, token.partial);
    }

    #[test]
    fn test_parse_text_with_camelcase() {
        let mut index = TestIndex::new();

        parse_text("FooBar123", &mut index);

        assert_eq!(4, index.tokens.len());

        let token = index.tokens.get(0).unwrap();
        assert_eq!("FooBar123", token.token);
        assert_eq!(0, token.line);
        assert_eq!(0, token.column);
        assert_eq!(0, token.offset);
        assert_eq!(false, token.partial);

        let token = index.tokens.get(1).unwrap();
        assert_eq!("Foo", token.token);
        assert_eq!(0, token.line);
        assert_eq!(0, token.column);
        assert_eq!(0, token.offset);
        assert_eq!(true, token.partial);

        let token = index.tokens.get(2).unwrap();
        assert_eq!("Bar", token.token);
        assert_eq!(0, token.line);
        assert_eq!(0, token.column);
        assert_eq!(3, token.offset);
        assert_eq!(true, token.partial);

        let token = index.tokens.get(3).unwrap();
        assert_eq!("123", token.token);
        assert_eq!(0, token.line);
        assert_eq!(0, token.column);
        assert_eq!(6, token.offset);
        assert_eq!(true, token.partial);
    }

    #[test]
    fn test_parse_text_with_3byte_char() {
        let mut index = TestIndex::new();

        parse_text("福 福foo福bar福", &mut index);

        assert_eq!(7, index.tokens.len());

        let token = index.tokens.get(0).unwrap();
        assert_eq!("福", token.token);
        assert_eq!(0, token.line);
        assert_eq!(0, token.column);
        assert_eq!(0, token.offset);
        assert_eq!(false, token.partial);

        let token = index.tokens.get(1).unwrap();
        assert_eq!("福foo福bar福", token.token);
        assert_eq!(0, token.line);
        assert_eq!(2, token.column);
        assert_eq!(4, token.offset);
        assert_eq!(false, token.partial);

        let token = index.tokens.get(2).unwrap();
        assert_eq!("福", token.token);
        assert_eq!(0, token.line);
        assert_eq!(2, token.column);
        assert_eq!(4, token.offset);
        assert_eq!(true, token.partial);

        let token = index.tokens.get(3).unwrap();
        assert_eq!("foo", token.token);
        assert_eq!(0, token.line);
        assert_eq!(2, token.column);
        assert_eq!(7, token.offset);
        assert_eq!(true, token.partial);

        let token = index.tokens.get(4).unwrap();
        assert_eq!("福", token.token);
        assert_eq!(0, token.line);
        assert_eq!(2, token.column);
        assert_eq!(10, token.offset);
        assert_eq!(true, token.partial);

        let token = index.tokens.get(5).unwrap();
        assert_eq!("bar", token.token);
        assert_eq!(0, token.line);
        assert_eq!(2, token.column);
        assert_eq!(13, token.offset);
        assert_eq!(true, token.partial);

        let token = index.tokens.get(6).unwrap();
        assert_eq!("福", token.token);
        assert_eq!(0, token.line);
        assert_eq!(2, token.column);
        assert_eq!(16, token.offset);
        assert_eq!(true, token.partial);
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
        assert_eq!(false, token.partial);

        let token = index.tokens.get(1).unwrap();
        assert_eq!("bar", token.token);
        assert_eq!(0, token.line);
        assert_eq!(7, token.column);
        assert_eq!(16, token.offset);
        assert_eq!(false, token.partial);
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
        assert_eq!(false, token.partial);

        let token = index.tokens.get(1).unwrap();
        assert_eq!("bar", token.token);
        assert_eq!(2, token.line);
        assert_eq!(2, token.column);
        assert_eq!(9, token.offset);
        assert_eq!(false, token.partial);
    }

    #[test]
    fn test_parse_text_with_initialism() {
        let mut index = TestIndex::new();

        parse_text("XMLHttpRequest", &mut index);

        assert_eq!(4, index.tokens.len());

        let token = index.tokens.get(0).unwrap();
        assert_eq!("XMLHttpRequest", token.token);
        assert_eq!(0, token.line);
        assert_eq!(0, token.column);
        assert_eq!(0, token.offset);
        assert_eq!(false, token.partial);

        let token = index.tokens.get(1).unwrap();
        assert_eq!("XML", token.token);
        assert_eq!(0, token.line);
        assert_eq!(0, token.column);
        assert_eq!(0, token.offset);
        assert_eq!(true, token.partial);

        let token = index.tokens.get(2).unwrap();
        assert_eq!("Http", token.token);
        assert_eq!(0, token.line);
        assert_eq!(0, token.column);
        assert_eq!(3, token.offset);
        assert_eq!(true, token.partial);

        let token = index.tokens.get(3).unwrap();
        assert_eq!("Request", token.token);
        assert_eq!(0, token.line);
        assert_eq!(0, token.column);
        assert_eq!(7, token.offset);
        assert_eq!(true, token.partial);
    }

    #[test]
    fn test_parse_text_with_wide_initialism() {
        let mut index = TestIndex::new();

        parse_text("ÜÜÜÜttpÜequest", &mut index);

        assert_eq!(4, index.tokens.len());

        let token = index.tokens.get(0).unwrap();
        assert_eq!("ÜÜÜÜttpÜequest", token.token);
        assert_eq!(0, token.line);
        assert_eq!(0, token.column);
        assert_eq!(0, token.offset);
        assert_eq!(false, token.partial);

        let token = index.tokens.get(1).unwrap();
        assert_eq!("ÜÜÜ", token.token);
        assert_eq!(0, token.line);
        assert_eq!(0, token.column);
        assert_eq!(0, token.offset);
        assert_eq!(true, token.partial);

        let token = index.tokens.get(2).unwrap();
        assert_eq!("Üttp", token.token);
        assert_eq!(0, token.line);
        assert_eq!(0, token.column);
        assert_eq!(6, token.offset);
        assert_eq!(true, token.partial);

        let token = index.tokens.get(3).unwrap();
        assert_eq!("Üequest", token.token);
        assert_eq!(0, token.line);
        assert_eq!(0, token.column);
        assert_eq!(11, token.offset);
        assert_eq!(true, token.partial);
    }
}
