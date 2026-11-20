use amqprs::channel::{Channel, ConsumerMessage};
use amqprs::{AmqpDeliveryTag, BasicProperties, FieldValue, ShortStr};

/// The context of a single delivery. Contains all metadata about the message.
pub struct DeliveryContext {
    pub channel: Channel,
    pub delivery_tag: AmqpDeliveryTag,
    pub exchange: String,
    pub routing_key: String,
    pub redelivered: bool,
    pub properties: BasicProperties,
}

impl DeliveryContext {
    /// Number of times this message was returned to the queue after a failed delivery
    /// (nack/reject with requeue), read from the `x-delivery-count` header.
    /// Set by quorum queues; `None` if the header is absent (e.g. the first delivery
    /// or a classic queue).
    pub fn delivery_count(&self) -> Option<u64> {
        self.header_count("x-delivery-count")
    }

    /// Number of times this message was acquired by a consumer but returned without
    /// a failed delivery (e.g. channel closed or consumer cancelled), read from the
    /// `x-acquired-count` header. Set by quorum queues; `None` if the header is absent.
    pub fn acquired_count(&self) -> Option<u64> {
        self.header_count("x-acquired-count")
    }

    fn header_count(&self, key: &str) -> Option<u64> {
        let key = ShortStr::try_from(key).ok()?;
        match self.properties.headers()?.get(&key)? {
            FieldValue::b(v) => u64::try_from(*v).ok(),
            FieldValue::B(v) => Some(u64::from(*v)),
            FieldValue::s(v) => u64::try_from(*v).ok(),
            FieldValue::u(v) => Some(u64::from(*v)),
            FieldValue::I(v) => u64::try_from(*v).ok(),
            FieldValue::i(v) => Some(u64::from(*v)),
            FieldValue::l(v) => u64::try_from(*v).ok(),
            _ => None,
        }
    }
}

#[inline]
pub fn create_delivery_context(
    message: ConsumerMessage,
    channel: &Channel,
) -> (DeliveryContext, Vec<u8>) {
    let deliver = message
        .deliver
        .expect("ConsumerMessage must have deliver according to amqprs spec");
    let properties = message
        .basic_properties
        .expect("ConsumerMessage must have basic_properties according to amqprs spec");
    (
        DeliveryContext {
            delivery_tag: deliver.delivery_tag(),
            exchange: deliver.exchange().to_owned(),
            routing_key: deliver.routing_key().to_owned(),
            redelivered: deliver.redelivered(),
            properties,
            channel: channel.clone(),
        },
        message
            .content
            .expect("ConsumerMessage must have content according to amqprs spec"),
    )
}
