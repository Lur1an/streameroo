use lapin::types::AMQPValue;

#[macro_export]
macro_rules! field_table {
     () => {
         $crate::amqp::lapin::types::FieldTable::default()
     };

     ($(($key:expr, $value:expr)),* $(,)?) => {{
         let mut field_table = $crate::amqp::lapin::types::FieldTable::default();
         $(
             field_table.insert(
                 $crate::amqp::lapin::types::ShortString::from($key),
                 $crate::amqp::lapin::types::AMQPValue::from($value)
             );
         )*
         field_table
     }};
 }

pub enum XQueueType {
    Classic,
    Quorum,
    Stream,
}

impl From<XQueueType> for AMQPValue {
    fn from(x: XQueueType) -> Self {
        match x {
            XQueueType::Classic => AMQPValue::ShortString("classic".into()),
            XQueueType::Quorum => AMQPValue::ShortString("quorum".into()),
            XQueueType::Stream => AMQPValue::ShortString("stream".into()),
        }
    }
}

#[cfg(test)]
mod test {
    use lapin::types::{FieldTable, ShortString};
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn test_field_table() {
        let table = field_table!(
            ("x-queue-type", XQueueType::Quorum),
            ("x-delivery-limit", 6)
        );
        let expected = FieldTable::from(BTreeMap::from([
            (
                ShortString::from("x-queue-type"),
                AMQPValue::ShortString("quorum".into()),
            ),
            (ShortString::from("x-delivery-limit"), AMQPValue::from(6)),
        ]));
        assert_eq!(table, expected);
    }
}
