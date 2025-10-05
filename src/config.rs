#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ProcessorConfig {
    pub skip_malformed: bool,
    pub log_warnings: bool,
    pub decimal_precision: u32,
    pub max_tx_history: Option<usize>,
}

impl Default for ProcessorConfig {
    fn default() -> Self {
        ProcessorConfig {
            skip_malformed: true,
            log_warnings: true,
            decimal_precision: 4,
            max_tx_history: None,
        }
    }
}

impl ProcessorConfig {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self::default()
    }

    #[allow(dead_code)]
    pub fn production() -> Self {
        ProcessorConfig {
            skip_malformed: true,
            log_warnings: true,
            decimal_precision: 4,
            max_tx_history: Some(10_000_000),
        }
    }

    #[allow(dead_code)]
    pub fn strict() -> Self {
        ProcessorConfig {
            skip_malformed: false,
            log_warnings: false,
            decimal_precision: 4,
            max_tx_history: None,
        }
    }

    #[allow(dead_code)]
    pub fn with_skip_malformed(mut self, skip: bool) -> Self {
        self.skip_malformed = skip;
        self
    }

    #[allow(dead_code)]
    pub fn with_log_warnings(mut self, log: bool) -> Self {
        self.log_warnings = log;
        self
    }

    #[allow(dead_code)]
    pub fn with_precision(mut self, precision: u32) -> Self {
        self.decimal_precision = precision;
        self
    }

    #[allow(dead_code)]
    pub fn with_max_tx_history(mut self, max: Option<usize>) -> Self {
        self.max_tx_history = max;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ProcessorConfig::default();
        assert!(config.skip_malformed);
        assert!(config.log_warnings);
        assert_eq!(config.decimal_precision, 4);
    }

    #[test]
    fn test_builder_pattern() {
        let config = ProcessorConfig::new()
            .with_skip_malformed(false)
            .with_precision(2);

        assert!(!config.skip_malformed);
        assert_eq!(config.decimal_precision, 2);
    }

    #[test]
    fn test_production_config() {
        let config = ProcessorConfig::production();
        assert!(config.skip_malformed);
        assert!(config.log_warnings);
    }

    #[test]
    fn test_strict_config() {
        let config = ProcessorConfig::strict();
        assert!(!config.skip_malformed);
        assert!(!config.log_warnings);
    }
}
