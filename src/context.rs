use fnv::FnvHashMap;
use std::any::{Any, TypeId};

pub struct Store(FnvHashMap<TypeId, &'static (dyn Any + Send + Sync)>);

pub struct Context {
    /// A generic data storage for shared instances of types
    pub(crate) data: Store,
}

impl Context {
    pub fn new() -> Self {
        Self {
            data: Store(FnvHashMap::default()),
        }
    }

    pub fn data<D: Any + Send + Sync + 'static>(&mut self, data: D) {
        let data = Box::new(data);
        self.data.0.insert(TypeId::of::<D>(), Box::leak(data));
    }

    pub fn data_unchecked<D: Any + Send + Sync + 'static>(&self) -> &'static D {
        self.data_opt::<D>().unwrap()
    }

    pub fn data_opt<D: Any + Send + Sync + 'static>(&self) -> Option<&'static D> {
        self.data
            .0
            .get(&TypeId::of::<D>())
            .and_then(|x| x.downcast_ref::<D>())
    }
}
