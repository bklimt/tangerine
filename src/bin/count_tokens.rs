use tangerine::parse::{TokenProcessor, TokenSlice, parse_text};

struct TokenPrinter {}

impl TokenProcessor for TokenPrinter {
    fn process_token(&mut self, token: &TokenSlice) {
        let word = token.token;
        println!("{word}")
    }
}

fn main() {
    let mut printer = TokenPrinter {};
    parse_text("foo bar baz", &mut printer);
}
