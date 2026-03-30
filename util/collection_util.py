import logging
from typing import Any

logger = logging.getLogger(__name__)


class CollectionUtil:

    @classmethod
    def get_unduplicate_value_from_list(cls, first_list, second_list) -> list[Any]:
        return [x for x in first_list if x not in second_list]