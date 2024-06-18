use std::{collections::HashMap, fs::read};

use crate::sql_data_types::SerialType;
use anyhow::{bail, Result};
use nom::Err;

// Reference Documentation being used to implement sql parsing
// https://www.sqlite.org/lang.html

pub struct SqlColumn {
    pub name: String,
    pub data_type: SerialType,
}

/* Given a create table stmt, return the schema name and the serial types in vector
// I need to go from a string to something structured
// DSL -> Data Structure
// Tokens -> AST -> Data Structure
// string -> Lexer -> Tokens -> Parser (Grammar) -> AST
// BNF
*/

pub fn find_schema_from_create_stmt(stmt: &str) -> Result<Vec<SqlColumn>> {
    // TODO
    // construct lexer
    // call parser with lexer
    // interpret the AST to get schema
    todo!()
}

// What are the tokens the lexer needs to be able to create table via sql?
#[derive(Clone)]
pub enum Token {
    Create,
    Table,
    Literal(String),
    StringLiteral(String),
    BlobLiteral(String),
    NumericLiteral(String),
    If,
    Not,
    Exists,
    OpenParen,
    CloseParen,
    Comma,
    Period,
    EOF, // indicates end of token stream
}

struct Lexer {
    input: String,
    char_position: usize,
    curr_token: Option<Token>,
    next_token: Option<Token>,
}

impl Lexer {
    fn new(input: String) -> Lexer {
        Lexer {
            input,
            char_position: 0,
            curr_token: None,
            next_token: None,
        }
    }

    fn get_curr_token(&mut self) -> Result<Option<Token>> {
        match &self.curr_token {
            Some(token) => Ok(Some(token.clone())),
            None => {
                let read_curr_token = self.read_in_token()?;
                self.curr_token = Some(read_curr_token.clone());
                Ok(Some(read_curr_token))
            }
        }
    }

    fn peek(&mut self) -> Result<Option<Token>> {
        match self.next_token {
            Some(ref token) => Ok(Some(token.clone())),
            None => {
                let next_token = self.read_in_token()?;
                self.next_token = Some(next_token.clone());
                Ok(Some(next_token))
            }
        }
    }

    fn advance(&mut self) {
        self.curr_token = self.next_token.take();
    }

    fn read_in_token(&mut self) -> Result<Token> {
        if self.char_position == self.input.len() {
            return Ok(Token::EOF);
        }

        let mut read_keyword_lookup = HashMap::new();

        read_keyword_lookup.insert(
            'c',
            vec![
                ("create", Token::Create),
                ("current_time", Token::Literal("CURRENT_TIME".to_string())),
                ("current_date", Token::Literal("CURRENT_DATE".to_string())),
                (
                    "current_timestamp",
                    Token::Literal("CURRENT_TIMESTAMP".to_string()),
                ),
            ],
        );

        read_keyword_lookup.insert('i', vec![("if", Token::If)]);

        read_keyword_lookup.insert(
            'n',
            vec![
                ("not", Token::Not),
                ("null", Token::Literal("NULL".to_string())),
            ],
        );

        read_keyword_lookup.insert('e', vec![("exists", Token::Exists)]);
        read_keyword_lookup.insert(
            't',
            vec![
                ("true", Token::Literal("TRUE".to_string())),
                ("table", Token::Table),
            ],
        );

        // get current character, and make a decision off of that
        let curr_char = self.input.chars().nth(self.char_position);
        match curr_char {
            Some('(') => {
                self.char_position += 1;
                Ok(Token::OpenParen)
            }
            Some(')') => {
                self.char_position += 1;
                Ok(Token::CloseParen)
            }
            Some(',') => {
                self.char_position += 1;
                Ok(Token::Comma)
            }
            Some('.') => {
                self.char_position += 1;
                Ok(Token::Period)
            }
            Some(' ') => {
                // skip whitespace, and recrusively call self
                self.char_position += 1;
                return self.read_in_token();
            }
            Some(c) => match read_keyword_lookup.get(&c) {
                Some(potential_expected_paths) => {
                    for (expected, token) in potential_expected_paths {
                        match self.read_and_return(&expected, token.clone()) {
                            Ok(token) => return Ok(token),
                            Err(_) => continue,
                        }
                    }
                    bail!("no corresponding token found for character")
                }
                None => {
                    match c {
                        '\'' => {
                            // string
                            // we need to find the closing single quote
                            let mut string_literal = String::new();
                            loop {
                                let next_char = self.input.chars().nth(self.char_position);
                                match next_char {
                                    Some('\'') => {
                                        self.char_position += 1;
                                        break;
                                    }
                                    Some(ch) => {
                                        string_literal.push(ch);
                                        self.char_position += 1;
                                    }
                                    None => bail!("Unexpected end of input"),
                                }
                            }
                            Ok(Token::StringLiteral(string_literal))
                        }, 
                        'x' | 'X' => {
                            // BLOB literal
                            // skip the X
                            self.char_position += 1;
                            match self.read_in_token()? {
                                Token::StringLiteral(literal) => {
                                    // check if the literal is a valid hex string
                                    if literal.len() % 2 != 0 {
                                        bail!("Invalid hex string")
                                    }
                                    for c in literal.chars() {
                                        if !c.is_ascii_hexdigit() {
                                            bail!("Invalid hex string")
                                        }
                                    }
                                    Ok(Token::BlobLiteral(literal))
                                },
                                _ => bail!("Unexpected character")
                            }
                        },
                        _ => {
                            // numeric literal
                            todo!()
                        }
                    }
                } 
                  // https://www.sqlite.org/syntax/literal-value.html
            },
            None => bail!("Unexpected character"),
        }
    }

    fn read_and_return(&mut self, expected: &str, token: Token) -> Result<Token> {
        let mut expected_chars = expected.chars();
        let mut curr_char = self.input.chars().nth(self.char_position);
        while let Some(expected_char) = expected_chars.next() {
            match curr_char {
                Some(c) => {
                    if c.to_lowercase().next() != expected_char.to_lowercase().next() {
                        bail!("Unexpected character")
                    }
                }
                None => bail!("Length of expected string does not match the input string"),
            }
            self.char_position += 1;
            curr_char = self.input.chars().nth(self.char_position);
        }
        Ok(token)
    }
}

// What is the BNF grammar for the create table statement
fn parser(lexer: &mut Lexer) -> Result<()> {
    todo!();
}
