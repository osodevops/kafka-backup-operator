//! Reconcilers for Kafka Backup CRDs
//!
//! This module contains the business logic for reconciling each CRD type.
//! Reconcilers are responsible for:
//! - Validating CRD specs
//! - Executing backup/restore/reset operations
//! - Updating resource status

pub mod backup;
pub mod offset_reset;
pub mod offset_rollback;
pub mod restore;
