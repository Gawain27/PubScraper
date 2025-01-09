from typing import Final


class AdapterPropertiesConstants:
    IFACE_REF: Final = "iface_ref"
    PHASE_REF: Final = "phase_ref"
    IFACE_FX: Final = "iface_fx"
    IFACE_FX_PARAM_LIST: Final = "iface_fx_param_list"
    IFACE_ADDITIONAL_FX = "iface_add_fx"
    MULTI_RESULT = "multi_result" # Returns a list of persistable entities
    # tell which entity is expected for next phase
    EXPECTED_ID: Final = "expected_id"
    ROLL_OVER_DEPTH: Final = "roll_over_depth"
