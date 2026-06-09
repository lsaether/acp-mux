mod actor;
pub mod registry;

pub use actor::{
    AttachError, MuxCore, MuxHandle, MuxMsg, MuxOptions, MuxSnapshot, ReplayView,
    SubscriberSnapshot, spawn_mux, spawn_mux_with_extension,
};
pub use registry::{AgentCmd, ControlPlaneSessionListError, MuxRegistry, RegistryError};
