
from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations, product
from typing import Any, Dict, Iterable, Iterator, List, Tuple

# Import your existing Templates class as-is
from old.template_old import Templates


# ---------- Types & helpers over your drug dictionary ----------
# Expected structure:
# drug_index = {
#   "RxNorm": {"1154343": "Hydrochlorothiazide 25 MG Oral Tablet", "1191": "Lisinopril 10 MG Oral Tablet"},
#   "ATC": {"C10AA05": "Atorvastatin"}
# }

DrugEntry = Tuple[str, str, str]  # (vocabulary_id, concept_code, drug_name)


def iter_pairs_within_vocab(
    drug_index: Dict[str, Dict[str, str]]
) -> Iterator[Tuple[DrugEntry, DrugEntry]]:
    for vocab, code_to_name in drug_index.items():
        entries = [(vocab, code, name) for code, name in code_to_name.items()]
        for a, b in combinations(entries, 2):
            yield a, b


def iter_pairs_cross_vocab(
    drug_index: Dict[str, Dict[str, str]]
) -> Iterator[Tuple[DrugEntry, DrugEntry]]:
    vocabs = list(drug_index.keys())
    for i in range(len(vocabs)):
        for j in range(i + 1, len(vocabs)):
            v1, v2 = vocabs[i], vocabs[j]
            for a, b in product(
                [(v1, c, n) for c, n in drug_index[v1].items()],
                [(v2, c, n) for c, n in drug_index[v2].items()],
            ):
                yield a, b


# ---------- Minimal spec to scale across many templates ----------
@dataclass(frozen=True)
class TemplateSpec:
    method: str                    # name of method on Templates
    roles: Tuple[str, ...]         # semantic roles used to generate inputs
    defaults: Dict[str, Any] | None = None


# Register templates you want to expand dynamically
TEMPLATES: Dict[str, TemplateSpec] = {
    # OR: two drugs (unordered pair)
    "patients_drugs_or": TemplateSpec(
        method="patients_drugs_or",
        roles=("drug_pair",),
    ),
    # AND within time window: two drugs + days
    "patients_drugs_and_time": TemplateSpec(
        method="patients_drugs_and_time",
        roles=("drug_pair", "days"),
        defaults={"days_pool": (7, 14, 30)},
    ),
    # Add more here as needed, just declare the roles they use
}


# ---------- Value providers for roles ----------
def provide_values_for_role(
    role: str,
    *,
    drug_index: Dict[str, Dict[str, str]],
    include_cross_vocab: bool,
    overrides: Dict[str, Any],
) -> Iterable[Any]:
    if role == "drug_pair":
        iters = [iter_pairs_within_vocab(drug_index)]
        if include_cross_vocab:
            iters.append(iter_pairs_cross_vocab(drug_index))
        for it in iters:
            for pair in it:
                yield pair  # ((v1, code1, name1), (v2, code2, name2))
    elif role == "days":
        # default pool, can be overridden when calling expand_and_call
        defaults = overrides.get("defaults") or {}
        days_pool = overrides.get("days_pool") or defaults.get("days_pool") or (7, 14, 30)
        for d in days_pool:
            yield int(d)
    else:
        raise ValueError(f"Unknown role '{role}'")


# ---------- Generic expander & caller ----------
def expand_and_call(
    template_key: str,
    *,
    templates_obj: Templates,
    drug_index: Dict[str, Dict[str, str]],
    include_cross_vocab: bool = False,
    **overrides: Any,
) -> Iterator[Tuple[str, Dict[str, Any], str, Dict[str, Any]]]:
    """
    Yields (sql, params, description, meta) for each expanded combination.
    - sql: SQL text from your Templates method
    - params: dict of psycopg2 parameters for safe execution
    - description: nice human-readable description
    - meta: extra fields like drug names, days, vocab/codes
    """
    spec = TEMPLATES.get(template_key)
    if not spec:
        raise ValueError(f"Unknown template '{template_key}'")

    method = getattr(templates_obj, spec.method, None)
    if not callable(method):
        raise RuntimeError(f"Templates method '{spec.method}' not found")

    # Build pools for each role
    role_pools: List[List[Any]] = []
    for role in spec.roles:
        pool = list(
            provide_values_for_role(
                role,
                drug_index=drug_index,
                include_cross_vocab=include_cross_vocab,
                overrides={"defaults": spec.defaults, **overrides},
            )
        )
        role_pools.append(pool)

    # Cartesian product of role values
    from itertools import product as cart_product
    for combo in cart_product(*role_pools) if role_pools else [()]:
        kwargs: Dict[str, Any] = {}
        meta: Dict[str, Any] = {}

        # Map semantic roles to kwargs expected by your Templates methods
        for role, value in zip(spec.roles, combo):
            if role == "drug_pair":
                (v1, code1, name1), (v2, code2, name2) = value
                kwargs.update(dict(v_id1=v1, d_id1=code1, v_id2=v2, d_id2=code2))
                meta.update(
                    dict(
                        drug1_name=name1,
                        drug2_name=name2,
                        v_id1=v1,
                        d_id1=code1,
                        v_id2=v2,
                        d_id2=code2,
                    )
                )
            elif role == "days":
                kwargs["days"] = int(value)
                meta["days"] = int(value)

        # Call your Templates method. Support (sql, params) or (sql, params, description)
        result = method(**kwargs)
        if not isinstance(result, tuple):
            raise TypeError(f"{spec.method} must return tuple, got {type(result)}")

        if len(result) == 3:
            sql, params, description = result
        elif len(result) == 2:
            sql, params = result
            # Build description if method doesn't provide one
            if template_key == "patients_drugs_or":
                description = f"Count of patients taking {meta['drug1_name']} OR {meta['drug2_name']}."
            elif template_key == "patients_drugs_and_time":
                description = (
                    f"Count of patients taking {meta['drug1_name']} AND {meta['drug2_name']} within {meta['days']} days."
                )
            else:
                description = spec.method
        else:
            raise TypeError(f"{spec.method} must return (sql, params) or (sql, params, description)")

        yield sql, params, description, meta