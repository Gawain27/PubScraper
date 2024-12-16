from typing import Final


class AdapterPropertiesConstants:
    IFACE_REF: Final = "iface_ref"
    PHASE_REF: Final = "phase_ref"
    IFACE_FX: Final = "iface_fx"
    IFACE_FX_PARAM: Final = "iface_fx_param"
    IFACE_FX_OPT_PARAM: Final = "iface_fx_opt_param"
    IFACE_IDX: Final = "iface_idx"
    IFACE_CACHED_ITER: Final = "iface_cached_iter"
    IFACE_ADDITIONAL_FX = "iface_add_fx"
    IFACE_IS_ITERATOR = "iface_is_iterator"
    MULTI_RESULT = "multi_result" # Returns a list of persistable entities
    ALT_ITERABLE: Final = "alt_iterable"  # Must be a list, complex types are not expected
    NEXT_PHASES: Final = "next_phases"  # Must be a list, multiple started per iteration
    # tell which entity is expected for next phase, on first phase is None.
    EXPECTED_ID: Final = "expected_id"
    NEXT_PHASE_DEPTH: Final = "next_phase_depth"
    ROLL_OVER_DEPTH: Final = "roll_over_depth"

