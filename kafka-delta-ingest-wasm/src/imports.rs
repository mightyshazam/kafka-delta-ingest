use kafka_delta_ingest_wasm_types::Status;
use num_traits::ToPrimitive;
use rdkafka::Message;
use wasmtime::{AsContext, AsContextMut, Caller, Extern, Linker};

use crate::HostContext;
const MEMORY_EXPORT_NAME: &str = "memory";

pub(crate) fn link(
    linker: &mut Linker<HostContext>,
    host_module: &str,
) -> Result<(), crate::Error> {
    linker.func_wrap(host_module, "proxy_log", proxy_log)?;
    linker.func_wrap(host_module, "proxy_get_log_level", proxy_get_log_level)?;
    linker.func_wrap(
        host_module,
        "proxy_get_current_time_nanoseconds",
        proxy_get_current_time_nanoseconds,
    )?;
    linker.func_wrap(
        host_module,
        "proxy_set_tick_period_milliseconds",
        proxy_set_tick_period_milliseconds,
    )?;
    linker.func_wrap(host_module, "proxy_set_output", proxy_set_output)?;
    linker.func_wrap(
        host_module,
        "proxy_get_message_body",
        proxy_get_message_body,
    )?;
    linker.func_wrap(
        host_module,
        "proxy_get_buffer_bytes",
        proxy_get_buffer_bytes,
    )?;
    linker.func_wrap(host_module, "proxy_get_property", proxy_get_property)?;
    Ok(())
}

fn proxy_log(mut env: Caller<HostContext>, offset: i32, _length: i32) {
    let memory = match env.get_export(MEMORY_EXPORT_NAME) {
        Some(Extern::Memory(memory)) => memory,
        _ => panic!("no memory exported from wasm module"),
    };
    let store = env.as_context_mut();
    let data = store.data().clone();
    let map = data.messages.read().unwrap();
    let payload = match map.get(&1) {
        Some(message) => message.message.payload().unwrap(),
        None => return,
    };
    memory.write(store, offset as usize, payload).unwrap();
}

fn proxy_get_log_level(mut env: Caller<HostContext>, offset: i32, _length: i32) {
    let memory = match env.get_export(MEMORY_EXPORT_NAME) {
        Some(Extern::Memory(memory)) => memory,
        _ => panic!("no memory exported from wasm module"),
    };
    let store = env.as_context_mut();
    let data = store.data().clone();
    let map = data.messages.read().unwrap();
    let payload = match map.get(&1) {
        Some(message) => message.message.payload().unwrap(),
        None => return,
    };
    memory.write(store, offset as usize, payload).unwrap();
}

fn proxy_get_current_time_nanoseconds(mut env: Caller<HostContext>, return_time: i64) -> i32 {
    let memory = match env.get_export(MEMORY_EXPORT_NAME) {
        Some(Extern::Memory(memory)) => memory,
        _ => panic!("no memory exported from wasm module"),
    };
    let store = env.as_context_mut();
    let result = match chrono::Utc::now().timestamp_nanos_opt() {
        Some(ts) => {
            memory
                .write(store, return_time as usize, &ts.to_le_bytes())
                .expect("unable to write timestamp in nanoseconds");
            Status::Ok
        }
        None => Status::InternalFailure,
    };
    result.to_i32().unwrap()
}

fn proxy_set_tick_period_milliseconds(mut env: Caller<HostContext>, interval: u64) {
    unimplemented!()
}

fn proxy_set_output(mut env: Caller<HostContext>, offset: i32, length: i32) {
    let memory = match env.get_export(MEMORY_EXPORT_NAME) {
        Some(Extern::Memory(memory)) => memory,
        _ => panic!("no memory exported from wasm module"),
    };
    let mut data = vec![0; length as usize];
    memory
        .read(env.as_context(), offset as usize, &mut data)
        .unwrap();
    env.data().save_response(1, data);
}

fn proxy_get_message_body(mut env: Caller<HostContext>, offset: i32, _length: i32) {
    let memory = match env.get_export(MEMORY_EXPORT_NAME) {
        Some(Extern::Memory(memory)) => memory,
        _ => panic!("no memory exported from wasm module"),
    };
    let store = env.as_context_mut();
    let data = store.data().clone();
    let map = data.messages.read().unwrap();
    let payload = match map.get(&1) {
        Some(message) => message.message.payload().unwrap(),
        None => return,
    };
    memory.write(store, offset as usize, payload).unwrap();
}

fn proxy_get_buffer_bytes(mut env: Caller<HostContext>, offset: i32, _length: i32) {
    let memory = match env.get_export(MEMORY_EXPORT_NAME) {
        Some(Extern::Memory(memory)) => memory,
        _ => panic!("no memory exported from wasm module"),
    };
    let store = env.as_context_mut();
    let data = store.data().clone();
    let map = data.messages.read().unwrap();
    let payload = match map.get(&1) {
        Some(message) => message.message.payload().unwrap(),
        None => return,
    };
    memory.write(store, offset as usize, payload).unwrap();
}

fn proxy_get_property(mut env: Caller<HostContext>, offset: i32, _length: i32) {
    let memory = match env.get_export(MEMORY_EXPORT_NAME) {
        Some(Extern::Memory(memory)) => memory,
        _ => panic!("no memory exported from wasm module"),
    };
    let store = env.as_context_mut();
    let data = store.data().clone();
    let map = data.messages.read().unwrap();
    let payload = match map.get(&1) {
        Some(message) => message.message.payload().unwrap(),
        None => return,
    };
    memory.write(store, offset as usize, payload).unwrap();
}
