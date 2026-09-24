use super::ValidationError;

pub fn validate_table_name(table_name: &str) -> Result<(), ValidationError> {
    if table_name.len() < 3 {
        return Err(ValidationError::TableNameValidationError(
            "Table name must contain at least 3 symbols".to_string(),
        ));
    }

    if table_name.len() > 63 {
        return Err(ValidationError::TableNameValidationError(
            "Table name must contain 3-63 symbols".to_string(),
        ));
    }

    let mut i = 0;

    let mut prev_char: Option<char> = None;

    let as_bytes = table_name.as_bytes();

    for s in as_bytes {
        let c = *s as char;

        if i == 0 {
            if c == '-' {
                return Err(ValidationError::TableNameValidationError(format!(
                    "Table can not be started from '-' symbol",
                )));
            }
        }

        if i == as_bytes.len() - 1 {
            if c == '-' {
                return Err(ValidationError::TableNameValidationError(format!(
                    "Table can not be ended with '-' symbol",
                )));
            }
        }

        if !symbol_is_allowed(c) {
            return Err(ValidationError::TableNameValidationError(format!(
                "Symbol {} is not allowed which stays at position {}",
                c, i
            )));
        }

        if c == '-' {
            if let Some(prev_char) = prev_char {
                if prev_char == '-' {
                    return Err(ValidationError::TableNameValidationError(format!(
                        "Two following '-' symbols are not allowed. Check please position {}",
                        i
                    )));
                }
            }
        }

        prev_char = Some(c);
        i += 1;
    }

    Ok(())
}

fn symbol_is_allowed(c: char) -> bool {
    c == '-' || is_digit(c) || is_lower_case_latin_letter(c)
}

fn is_digit(c: char) -> bool {
    return c >= '0' && c <= '9';
}

fn is_lower_case_latin_letter(c: char) -> bool {
    return c >= 'a' && c <= 'z';
}

#[cfg(test)]
mod test {
    use crate::validations::ValidationError;

    #[test]
    fn test_lower_cases_and_dashes_ok() {
        let test_table_name = "my-test-table-5";

        let result = super::validate_table_name(test_table_name);

        assert_eq!(true, result.is_ok());
    }

    #[test]
    fn test_lower_cases_and_two_dashes() {
        let test_table_name = "my-test--table";

        let result = super::validate_table_name(test_table_name);

        assert_eq!(false, result.is_ok());

        if let Err(err) = result {
            match err {
                ValidationError::TableNameValidationError(reason) => {
                    println!("{}", reason);
                }
            }
        }
    }

    #[test]
    fn test_lower_cases_and_start_with_dash() {
        let test_table_name = "-my-test-table";

        let result = super::validate_table_name(test_table_name);

        assert_eq!(false, result.is_ok());

        if let Err(err) = result {
            match err {
                ValidationError::TableNameValidationError(reason) => {
                    println!("{}", reason);
                }
            }
        }
    }

    #[test]
    fn test_lower_cases_and_ended_with_dash() {
        let test_table_name = "my-test-table-";

        let result = super::validate_table_name(test_table_name);

        assert_eq!(false, result.is_ok());

        if let Err(err) = result {
            match err {
                ValidationError::TableNameValidationError(reason) => {
                    println!("{}", reason);
                }
            }
        }
    }

    #[test]
    fn test_upper_cases_and_ended_with_dash() {
        let test_table_name = "my-test-Table";

        let result = super::validate_table_name(test_table_name);

        assert_eq!(false, result.is_ok());

        if let Err(err) = result {
            match err {
                ValidationError::TableNameValidationError(reason) => {
                    println!("{}", reason);
                }
            }
        }
    }
}
