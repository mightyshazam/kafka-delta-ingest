use crate::traits::*;

pub type NewRootContext = fn(context_id: u32) -> Box<dyn RootContext>;
pub type NewMessageContext = fn(context_id: u32, root_context_id: u32) -> Box<dyn MessageContext>;
