import re
from typing import Tuple, Dict, Any


def apply_validations(row: Dict[str, Any], schema: Dict) -> Tuple[bool, Dict]:
    errors = {}
    for col, rules in schema.get('columns', {}).items():
        val = row.get(col)
        if rules.get('required') and (val is None or val == ''):
            errors[col] = 'missing'
            continue
        if val is None or val == '':
            continue
        typ = rules.get('type')
        if typ == 'int':
            try:
                int(val)
            except Exception:
                errors[col] = 'not_int'
                continue
        if typ == 'float':
            try:
                float(val)
            except Exception:
                errors[col] = 'not_float'
                continue
        if 'regex' in rules:
            if not re.match(rules['regex'], str(val)):
                errors[col] = 'regex_mismatch'
                continue
        if 'allowed_values' in rules:
            if val not in rules['allowed_values']:
                errors[col] = 'not_allowed'
                continue
        if 'min' in rules:
            try:
                if float(val) < float(rules['min']):
                    errors[col] = 'below_min'
                    continue
            except Exception:
                pass
        if 'max' in rules:
            try:
                if float(val) > float(rules['max']):
                    errors[col] = 'above_max'
                    continue
            except Exception:
                pass
    return (len(errors) == 0, errors)
