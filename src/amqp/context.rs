use amqprs::channel::{Channel, ConsumerMessage};
use amqprs::{AmqpDeliveryTag, BasicProperties};

/// The context of a single delivery. Contains all metadata about the message.
pub struct DeliveryContext {
    pub channel: Channel,
    pub delivery_tag: AmqpDeliveryTag,
    pub exchange: String,
    pub routing_key: String,
    pub redelivered: bool,
    pub properties: BasicProperties,
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
