#[derive(Debug, Clone, PartialEq)]
pub enum FieldType {
    Serial,
    BigSerial,
    Integer,
    BigInt,
    SmallInt,
    Text,
    VarChar(usize),
    Boolean,
    Timestamp,
    TimestampTz,
    Date,
    Time,
    Uuid,
    Json,
    JsonB,
    Binary,
    Real,
    DoublePrecision,
    Decimal { precision: u8, scale: u8 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_type_clone_and_eq() {
        let ft1 = FieldType::VarChar(255);
        let ft2 = ft1.clone();
        assert_eq!(ft1, ft2);
    }

    #[test]
    fn decimal_stores_precision_and_scale() {
        let ft = FieldType::Decimal {
            precision: 10,
            scale: 2,
        };
        if let FieldType::Decimal { precision, scale } = ft {
            assert_eq!(precision, 10);
            assert_eq!(scale, 2);
        } else {
            panic!("Expected Decimal variant");
        }
    }
}
