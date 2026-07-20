use solana_pubkey::Pubkey;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

pub const SUPPLY_RING_SLOTS: u64 = 128;

#[derive(Clone, Copy, Debug)]
struct TrackedLamports {
    slot: u64,
    lamports: u64,
}

#[derive(Clone, Default)]
pub struct SupplyTracker(Option<Arc<Inner>>);

struct Inner {
    state: Mutex<SupplyState>,
    accounts: RwLock<HashMap<Pubkey, TrackedLamports>>,
}

#[derive(Clone, Copy, Default, PartialEq)]
enum SupplyStatus {
    #[default]
    Bootstrapping,
    Live,
    Stale,
    ReanchorPending,
}

#[derive(Default)]
struct SupplyState {
    status: SupplyStatus,
    total: u64,
    slot: u64,
    anchor_slot: u64,
    pending_delta: i128,
    non_circulating_accounts: Option<Vec<Pubkey>>,
}

#[derive(Debug, Clone)]
pub struct SupplyCommit {
    pub slot: u64,
    pub total: u64,
    pub non_circulating: Option<u64>,
}

impl SupplyTracker {
    pub fn new() -> Self {
        Self(Some(Arc::new(Inner {
            state: Mutex::new(SupplyState::default()),
            accounts: RwLock::new(HashMap::new()),
        })))
    }

    pub fn is_enabled(&self) -> bool {
        self.0.is_some()
    }

    pub fn observe_account(&self, pubkey: &[u8], slot: u64, lamports: u64) {
        let Some(inner) = &self.0 else { return };
        let pubkey = Pubkey::try_from(pubkey).unwrap();
        let mut state = inner.state.lock().expect("Failed to lock supply state");
        state.slot = state.slot.max(slot);

        let mut map = inner
            .accounts
            .write()
            .expect("Failed to write supply accounts");
        let previous_lamports = match map.get(&pubkey) {
            Some(entry) if entry.slot > slot => return,
            Some(entry) => entry.lamports,
            None => 0,
        };
        if lamports == 0 && state.status == SupplyStatus::Live {
            map.remove(&pubkey);
        } else {
            map.insert(pubkey, TrackedLamports { slot, lamports });
        }
        drop(map);

        if state.status != SupplyStatus::Live {
            return;
        }
        state.pending_delta += lamports as i128 - previous_lamports as i128;
    }

    pub fn observe_snapshot_account(&self, pubkey: &Pubkey, slot: u64, lamports: u64) {
        let Some(inner) = &self.0 else { return };
        let mut map = inner
            .accounts
            .write()
            .expect("Failed to write supply accounts");
        match map.get(pubkey) {
            Some(entry) if entry.slot >= slot => {}
            _ => {
                map.insert(*pubkey, TrackedLamports { slot, lamports });
            }
        }
    }

    pub fn set_non_circulating_accounts(&self, accounts: Vec<Pubkey>) {
        let Some(inner) = &self.0 else { return };
        inner
            .state
            .lock()
            .expect("Failed to lock supply state")
            .non_circulating_accounts = Some(accounts);
    }

    pub fn note_anchor(&self, slot: u64) {
        let Some(inner) = &self.0 else { return };
        let mut state = inner.state.lock().expect("Failed to lock supply state");
        state.anchor_slot = state.anchor_slot.max(slot);
    }

    pub fn finish_bootstrap(&self) -> Option<SupplyCommit> {
        let inner = self.0.as_deref()?;
        let mut state = inner.state.lock().expect("Failed to lock supply state");
        if state.status != SupplyStatus::Bootstrapping || state.anchor_slot == 0 {
            return None;
        }
        state.total = inner.sweep_and_sum();
        state.slot = state.slot.max(state.anchor_slot);
        state.status = SupplyStatus::Live;
        state.pending_delta = 0;
        Some(inner.commit(&state))
    }

    pub fn mark_stale(&self) -> bool {
        let Some(inner) = &self.0 else {
            return false;
        };
        let mut state = inner.state.lock().expect("Failed to lock supply state");
        if state.status != SupplyStatus::Live {
            return false;
        }
        state.status = SupplyStatus::Stale;
        state.pending_delta = 0;
        true
    }

    pub fn request_reanchor(&self) {
        let Some(inner) = &self.0 else { return };
        let mut state = inner.state.lock().expect("Failed to lock supply state");
        if state.status == SupplyStatus::Stale {
            state.status = SupplyStatus::ReanchorPending;
        }
    }

    pub fn commit_block(&self, slot: u64) -> Option<SupplyCommit> {
        let inner = self.0.as_deref()?;
        let mut state = inner.state.lock().expect("Failed to lock supply state");
        state.slot = state.slot.max(slot);
        match state.status {
            SupplyStatus::Bootstrapping | SupplyStatus::Stale => return None,
            SupplyStatus::ReanchorPending => {
                state.total = inner.sweep_and_sum();
                state.status = SupplyStatus::Live;
            }
            SupplyStatus::Live => {
                state.total = (state.total as i128 + state.pending_delta) as u64;
            }
        }
        state.pending_delta = 0;
        Some(inner.commit(&state))
    }
}

impl Inner {
    fn commit(&self, state: &SupplyState) -> SupplyCommit {
        SupplyCommit {
            slot: state.slot,
            total: state.total,
            non_circulating: self.sum_non_circulating(state),
        }
    }

    fn sum_non_circulating(&self, state: &SupplyState) -> Option<u64> {
        let accounts = state.non_circulating_accounts.as_ref()?;
        let map = self
            .accounts
            .read()
            .expect("Failed to read supply accounts");
        let lamports: u128 = accounts
            .iter()
            .map(|pubkey| map.get(pubkey).map_or(0, |entry| entry.lamports as u128))
            .sum();
        Some(lamports as u64)
    }

    fn sweep_and_sum(&self) -> u64 {
        let mut map = self
            .accounts
            .write()
            .expect("Failed to write supply accounts");
        let mut total: u128 = 0;
        map.retain(|_, entry| {
            total += entry.lamports as u128;
            entry.lamports > 0
        });
        total as u64
    }
}
