use kafka_delta_ingest_wasm_types::Action;
use num_traits::FromPrimitive;
use wasmtime::{AsContextMut, Instance, Store, StoreContextMut, TypedFunc};

use crate::HostContext;

pub(crate) struct ExportCollection {
    _on_message_received: TypedFunc<(i32, u32), i32>,
    _on_context_create: TypedFunc<(i32, i32), ()>,
}

impl ExportCollection {
    pub(crate) fn new(
        instance: &Instance,
        store: &mut Store<HostContext>,
    ) -> Result<Self, crate::Error> {
        let on_message_received = instance.get_typed_func::<(i32, u32), i32>(
            store.as_context_mut(),
            "proxy_on_message_received",
        )?;
        let on_context_create = instance
            .get_typed_func::<(i32, i32), ()>(store.as_context_mut(), "proxy_on_context_create")?;
        Ok(Self {
            _on_message_received: on_message_received,
            _on_context_create: on_context_create,
        })
    }

    pub(crate) fn trigger_context_create(
        &self,
        store: &mut Store<HostContext>,
        context_id: i32,
        root_context_id: i32,
    ) -> Result<(), crate::Error> {
        self._on_context_create
            .call(store, (context_id, root_context_id))?;
        Ok(())
    }

    pub(crate) fn on_message_received(
        &self,
        store: StoreContextMut<HostContext>,
        context_id: i32,
        length: usize,
    ) -> Result<Action, crate::Error> {
        let result = self
            ._on_message_received
            .call(store, (context_id, length as u32))?;
        let enum_option: Option<Action> = FromPrimitive::from_i32(result);
        Ok(enum_option.unwrap_or(Action::Use))
    }
}
