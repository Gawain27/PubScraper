from typing import Final


class AdapterPropertiesConstants:
    IFACE_REF: Final = "iface_ref"
    PHASE_REF: Final = "phase_ref"
    IFACE_FX: Final = "iface_fx"
    IFACE_FX_PARAM: Final = "iface_fx_param"
    IFACE_IDX: Final = "iface_idx"
    IFACE_CACHED_ITER: Final = "iface_cached_iter"
    IFACE_ADDITIONAL_FX = "iface_add_fx"
    ALT_ITERABLE: Final = "alt_iterable"  # Must be a list, complex types are not expected
    NEXT_PHASES: Final = "next_phases"  # Must be a list, multiple started per iteration
    # tell which entity is expected for next phase, on first phase is None.
    NEXT_PHASE_ID: Final = "next_phase_id"
    NEXT_PHASE_INPUT: Final = "next_phase_input"

