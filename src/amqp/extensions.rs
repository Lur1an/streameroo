use amqprs::FieldValue;

#[macro_export]
macro_rules! field_table {
     () => {
         $crate::amqp::amqprs::FieldTable::default()
     };

     ($(($key:expr, $value:expr)),* $(,)?) => {{
         let mut field_table = $crate::amqp::amqprs::FieldTable::default();
         $(
             field_table.insert(
                 $crate::amqp::amqprs::FieldName::try_from($key).unwrap(),
                 $crate::amqp::amqprs::FieldValue::from($value)
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

impl From<XQueueType> for FieldValue {
    fn from(x: XQueueType) -> Self {
        match x {
            XQueueType::Classic => FieldValue::from("classic"),
            XQueueType::Quorum => FieldValue::from("quorum"),
            XQueueType::Stream => FieldValue::from("stream"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::amqp::connection::amqp_test::AMQPTest;
    use amqprs::channel::QueueDeclareArguments;
    use amqprs::{FieldName, FieldTable};
    use test_context::test_context;

    #[test_context(AMQPTest)]
    #[tokio::test]
    async fn test_field_table_quorum(ctx: &mut AMQPTest) -> anyhow::Result<()> {
        let channel = ctx.connection.open_channel().await?;
        let mut options = QueueDeclareArguments::default();
        options
            .queue("queue".to_owned())
            .durable(true)
            .arguments(field_table!(
                ("x-queue-type", XQueueType::Quorum),
                ("x-delivery-limit", FieldValue::u(6)),
            ));
        let queue = channel.queue_declare(options).await?.unwrap();
        assert_eq!(queue.0, "queue");
        Ok(())
    }

    #[test]
    fn test_field_table() {
        let table = field_table!(
            ("x-queue-type", XQueueType::Quorum),
            ("x-delivery-limit", FieldValue::u(6))
        );
        let mut expected = FieldTable::default();
        expected.insert(
            FieldName::try_from("x-queue-type").unwrap(),
            FieldValue::from("quorum"),
        );
        expected.insert(
            FieldName::try_from("x-delivery-limit").unwrap(),
            FieldValue::u(6),
        );
        assert_eq!(table, expected);
    }
}
