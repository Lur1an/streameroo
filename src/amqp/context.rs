use fnv::FnvHashMap;
use lapin::acker::Acker;
use lapin::message::Delivery;
use lapin::types::{DeliveryTag, ShortString};
use lapin::{BasicProperties, Channel};
use std::any::{Any, TypeId};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

type Store = FnvHashMap<TypeId, &'static (dyn Any + Send + Sync)>;

pub struct Context {
    /// The global lapin channel to interact with the broker
    pub(crate) channel: Channel,
    /// A generic data storage for shared instances of types
    pub(crate) data: Store,
}

pub struct Exchange(String);

impl Exchange {
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl Deref for Exchange {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Context {
    pub fn new(channel: Channel) -> Self {
        Self {
            channel,
            data: FnvHashMap::default(),
        }
    }

    pub fn data<D: Any + Send + Sync + 'static>(&mut self, data: D) {
        let data = Box::new(data);
        self.data.insert(TypeId::of::<D>(), Box::leak(data));
    }

    pub fn data_unchecked<D: Any + Send + Sync + 'static>(&self) -> &'static D {
        self.data_opt::<D>().unwrap()
    }

    pub fn data_opt<D: Any + Send + Sync + 'static>(&self) -> Option<&'static D> {
        self.data
            .get(&TypeId::of::<D>())
            .and_then(|x| x.downcast_ref::<D>())
    }
}

/// The context of a Delivery. All values derivable from the derivable and global context can be accessed here.
pub struct DeliveryContext {
    /// Reference to the global context
    pub(crate) global: Arc<Context>,
    pub(crate) delivery_tag: DeliveryTag,
    pub(crate) exchange: ShortString,
    pub(crate) routing_key: ShortString,
    pub(crate) redelivered: bool,
    pub(crate) properties: BasicProperties,
    pub(crate) acker: Acker,
}

pub struct State<T: 'static>(&'static T);

impl<T> Deref for State<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<T> State<T> {
    pub fn into_inner(self) -> &'static T {
        self.0
    }
}

pub struct StateOwned<T>(pub T);

impl<T> Deref for StateOwned<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for StateOwned<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> StateOwned<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> FromDeliveryContext<'_> for StateOwned<T>
where
    T: Any + Send + Sync + Clone,
{
    fn from_delivery_context(context: &DeliveryContext) -> Self {
        let value = context.global.data_unchecked::<T>().clone();
        StateOwned(value)
    }
}

impl FromDeliveryContext<'_> for Exchange {
    fn from_delivery_context(context: &DeliveryContext) -> Self {
        Exchange(context.exchange.to_string())
    }
}

impl<T> FromDeliveryContext<'_> for State<T>
where
    T: Any + Send + Sync + 'static,
{
    fn from_delivery_context(context: &DeliveryContext) -> Self {
        State(context.global.data_unchecked::<T>())
    }
}

impl FromDeliveryContext<'_> for Channel {
    fn from_delivery_context(context: &DeliveryContext) -> Self {
        context.global.channel.clone()
    }
}

pub trait FromDeliveryContext<'a> {
    fn from_delivery_context(context: &'a DeliveryContext) -> Self;
}

#[inline]
pub fn create_delivery_context(
    delivery: Delivery,
    context: Arc<Context>,
) -> (DeliveryContext, Vec<u8>) {
    (
        DeliveryContext {
            global: context,
            delivery_tag: delivery.delivery_tag,
            exchange: delivery.exchange,
            routing_key: delivery.routing_key,
            redelivered: delivery.redelivered,
            properties: delivery.properties,
            acker: delivery.acker,
        },
        delivery.data,
    )
}
