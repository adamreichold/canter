use crate::{Index, error::Error};

impl Index {
    pub fn add_tokenizer(&mut self, name: String, tokenizer: Box<dyn ErasedTokenizer>) {
        self.tokenizers.insert(name, tokenizer);
    }
}

pub trait Tokenizer {
    fn tokenize<F>(&mut self, text: &str, f: F) -> Result<(), Error>
    where
        F: FnMut(&str) -> Result<(), Error>;

    fn chain<T>(self, tokenizer: T) -> ChainedTokenizer<Self, T>
    where
        Self: Sized,
    {
        ChainedTokenizer {
            inner: self,
            outer: tokenizer,
        }
    }
}

pub trait ErasedTokenizer {
    fn erased_tokenize(
        &mut self,
        text: &str,
        f: &mut dyn FnMut(&str) -> Result<(), Error>,
    ) -> Result<(), Error>;
}

impl<T> ErasedTokenizer for T
where
    T: Tokenizer,
{
    fn erased_tokenize(
        &mut self,
        text: &str,
        f: &mut dyn FnMut(&str) -> Result<(), Error>,
    ) -> Result<(), Error> {
        self.tokenize(text, f)
    }
}

impl<T> From<T> for Box<dyn ErasedTokenizer>
where
    T: Tokenizer + 'static,
{
    fn from(tokenizer: T) -> Self {
        Box::new(tokenizer)
    }
}

pub struct ChainedTokenizer<I, O> {
    inner: I,
    outer: O,
}

impl<I, O> Tokenizer for ChainedTokenizer<I, O>
where
    I: Tokenizer,
    O: Tokenizer,
{
    fn tokenize<F>(&mut self, text: &str, mut f: F) -> Result<(), Error>
    where
        F: FnMut(&str) -> Result<(), Error>,
    {
        self.inner
            .tokenize(text, |text| self.outer.tokenize(text, &mut f))
    }
}

pub struct StubTokenizer;

impl Tokenizer for StubTokenizer {
    fn tokenize<F>(&mut self, text: &str, mut f: F) -> Result<(), Error>
    where
        F: FnMut(&str) -> Result<(), Error>,
    {
        f(text)
    }
}

pub struct SplitNonAlphanumeric;

impl Tokenizer for SplitNonAlphanumeric {
    fn tokenize<F>(&mut self, text: &str, mut f: F) -> Result<(), Error>
    where
        F: FnMut(&str) -> Result<(), Error>,
    {
        for text in text.split(|char_: char| !char_.is_alphanumeric()) {
            if !text.is_empty() {
                f(text)?;
            }
        }

        Ok(())
    }
}

pub struct LimitLength {
    limit: usize,
}

impl LimitLength {
    pub fn new(limit: usize) -> Self {
        Self { limit }
    }
}

impl Default for LimitLength {
    fn default() -> Self {
        Self::new(40)
    }
}

impl Tokenizer for LimitLength {
    fn tokenize<F>(&mut self, text: &str, mut f: F) -> Result<(), Error>
    where
        F: FnMut(&str) -> Result<(), Error>,
    {
        if text.len() > self.limit {
            return Ok(());
        }

        f(text)
    }
}

#[derive(Default)]
pub struct ToLowerCase {
    buf: String,
}

impl Tokenizer for ToLowerCase {
    fn tokenize<F>(&mut self, text: &str, mut f: F) -> Result<(), Error>
    where
        F: FnMut(&str) -> Result<(), Error>,
    {
        self.buf.clear();
        self.buf.reserve(text.len());
        self.buf.extend(text.chars().flat_map(char::to_lowercase));

        f(&self.buf)
    }
}
