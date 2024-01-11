use std::cell::{Cell, RefCell};
use std::collections::HashMap;

use std::sync::{Arc, RwLock};

use exports::ExportCollection;
use kafka_delta_ingest_wasm_types::Action;
use rdkafka::message::OwnedMessage;
use rdkafka::Message;
use wasmtime::{AsContextMut, Engine, Instance, Linker, Module, Store, TypedFunc};

mod exports;
mod imports;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("webassembly startup error: {error}")]
    Startup { error: String },
    #[error("webassembly runtime error: {error}")]
    Runtime { error: String },
    #[error("invalide json from webassembly content: {source}")]
    Json {
        #[from]
        source: serde_json::Error,
    },
}

impl From<wasmtime::Error> for Error {
    fn from(value: wasmtime::Error) -> Self {
        Error::Startup {
            error: format!("{}", value),
        }
    }
}

impl From<wasmtime::MemoryAccessError> for Error {
    fn from(value: wasmtime::MemoryAccessError) -> Self {
        Error::Startup {
            error: format!("{}", value),
        }
    }
}

impl From<wasmtime::UnknownImportError> for Error {
    fn from(value: wasmtime::UnknownImportError) -> Self {
        Error::Startup {
            error: format!("{}", value),
        }
    }
}

#[derive(Clone, Default)]
struct HostContext {
    current_context_id: Cell<u32>,
    messages: Arc<RwLock<HashMap<u32, MessageContext>>>,
}

#[derive(Clone)]
struct MessageContext {
    message: OwnedMessage,
    response: RefCell<Option<Vec<u8>>>,
}

impl MessageContext {
    fn new(message: OwnedMessage) -> Self {
        Self {
            message,
            response: RefCell::new(None),
        }
    }
}

impl HostContext {
    fn add_message(&self, context_id: u32, message: OwnedMessage) {
        let mut map_ref = self.messages.write().unwrap();
        map_ref.insert(context_id, MessageContext::new(message));
    }

    fn remove_message(&self, context_id: u32) -> Option<MessageContext> {
        let mut map_ref = self.messages.write().unwrap();
        map_ref.remove(&context_id)
    }

    fn save_response(&self, context_id: u32, data: Vec<u8>) {
        let mut map_ref = self.messages.write().unwrap();
        if let Some(message_context) = map_ref.get_mut(&context_id) {
            message_context.response.replace(Some(data));
        }
    }
}

pub struct WasmTransformer {
    engine: Engine,
    module: Module,
    linker: Linker<HostContext>,
}

impl WasmTransformer {
    thread_local! {
        static HOST: RefCell<Option<WasmHost>> = RefCell::new(None);
    }

    pub fn try_from_file(file: impl AsRef<std::path::Path>) -> Result<Self, Error> {
        let engine = Engine::default();

        let module = Module::from_file(&engine, file)?;
        let mut linker = Linker::new(&engine);
        imports::link(&mut linker, "host")?;
        Ok(Self {
            engine,
            module,
            linker,
        })
    }

    pub fn process(&self, message: OwnedMessage) -> Result<Option<serde_json::Value>, Error> {
        WasmTransformer::HOST.with(|cell| {
            let mut borrowed = cell.borrow_mut();
            match borrowed.as_mut() {
                Some(host) => host.process(message),
                None => {
                    let new_host = self.new_wasm_host()?;
                    borrowed.replace(new_host);
                    borrowed.as_mut().unwrap().process(message)
                }
            }
        })
    }

    fn new_wasm_host(&self) -> Result<WasmHost, Error> {
        let engine = self.engine.clone();
        let module = self.module.clone();
        let linker = self.linker.clone();
        let data = HostContext::default();
        let mut store = Store::new(&engine, data);
        let instance = linker.instantiate(&mut store, &module)?;
        let export_collection = ExportCollection::new(&instance, &mut store)?;
        export_collection.trigger_context_create(&mut store, 1, 0)?;
        export_collection.trigger_context_create(&mut store, 1, 1)?;
        Ok(WasmHost {
            _instance: instance,
            store,
            _module: module,
            export_collection,
        })
    }
}

struct WasmHost {
    _instance: Instance,
    store: Store<HostContext>,
    _module: Module,
    export_collection: ExportCollection,
}

impl WasmHost {
    pub fn process(&mut self, message: OwnedMessage) -> Result<Option<serde_json::Value>, Error> {
        let context_id = 1;
        let mut_context = self.store.as_context_mut();
        let length = message.payload().unwrap_or_default().len();
        let host_context = mut_context.data().clone();
        host_context.current_context_id.replace(context_id);
        host_context.add_message(context_id, message);

        match self
            .export_collection
            .on_message_received(mut_context, 1, length)?
        {
            // discard
            Action::Use => Ok(None),
            _ => {
                let message_context = host_context.remove_message(context_id).unwrap();
                let message_content = message_context.response.borrow();
                let content = message_content
                    .as_deref()
                    .unwrap_or(message_context.message.payload().unwrap());
                let json = serde_json::de::from_slice::<serde_json::Value>(content)?;
                Ok(Some(json))
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use rdkafka::message::OwnedHeaders;
    use serde_json::Value;

    use super::*;

    #[test]
    fn proxy_sample_keep() {
        let ctx = WasmTransformer::try_from_file("tests/proxy_sample.wat")
            .expect("failed to load wat file");
        let message = OwnedMessage::new(
            Some(vec![5, 4, 3, 2, 1]),
            Some("test.into".as_bytes().to_vec()),
            "test".into(),
            rdkafka::Timestamp::NotAvailable,
            0,
            0,
            Some(OwnedHeaders::new()),
        );
        let result = ctx.process(message).unwrap();
        let content = result.expect("content should return");
        match content {
            Value::Object(_) => {}
            _ => panic!("should have returned an object"),
        }
    }

    #[test]
    fn proxy_sample_ignore() {
        let ctx = WasmTransformer::try_from_file("tests/proxy_sample_ignore.wat")
            .expect("failed to load wat file");
        let message = OwnedMessage::new(
            Some(vec![5, 4, 3, 2, 1]),
            Some("test.into".as_bytes().to_vec()),
            "test".into(),
            rdkafka::Timestamp::NotAvailable,
            0,
            0,
            Some(OwnedHeaders::new()),
        );
        if ctx.process(message).unwrap().is_some() {
            panic!("response should be none")
        }
    }

    #[test]
    fn proxy_sample_multiple_threads() {
        let ctx = Arc::new(
            WasmTransformer::try_from_file("tests/proxy_sample.wat")
                .expect("failed to load wat file"),
        );
        let clone = ctx.clone();
        let first = std::thread::spawn(move || {
            let message = OwnedMessage::new(
                Some(vec![5, 4, 3, 2, 1]),
                Some("test.into".as_bytes().to_vec()),
                "test".into(),
                rdkafka::Timestamp::NotAvailable,
                0,
                0,
                Some(OwnedHeaders::new()),
            );
            let result = ctx.process(message).unwrap();
            let content = result.expect("content should return");
            match content {
                Value::Object(_) => {}
                _ => panic!("should have returned an object"),
            };
        });
        let second = std::thread::spawn(move || {
            let message = OwnedMessage::new(
                Some(vec![5, 4, 3, 2, 1]),
                Some("test.into".as_bytes().to_vec()),
                "test".into(),
                rdkafka::Timestamp::NotAvailable,
                0,
                0,
                Some(OwnedHeaders::new()),
            );
            let result = clone.process(message).unwrap();
            let content = result.expect("content should return");
            match content {
                Value::Object(_) => {}
                _ => panic!("should have returned an object"),
            };
        });
        first.join().unwrap();
        second.join().unwrap();
    }
}
