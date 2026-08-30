"""
Basic MapReduce framework in pure Python.

This module provides a simple MapReduce implementation
to process data locally and understand the paradigm.
"""

from collections import defaultdict
from typing import Callable, Iterator, Tuple, Any, List


def mapreduce(
    data: List[Any],
    mapper: Callable[[Any], Iterator[Tuple[Any, Any]]],
    reducer: Callable[[Any, List[Any]], Any]
) -> dict:
    """
    Executes a complete MapReduce process.
    
    Args:
        data: List of items to process
        mapper: Function that transforms each item into (key, value) pairs
        reducer: Function that aggregates values by key
    
    Returns:
        Dictionary with final results {key: reduced_value}
    """
    # MAP phase
    mapped = []
    for item in data:
        mapped.extend(mapper(item))
    
    # SHUFFLE/SORT phase
    shuffled = defaultdict(list)
    for key, value in mapped:
        shuffled[key].append(value)
    
    # REDUCE phase
    reduced = {}
    for key, values in shuffled.items():
        reduced[key] = reducer(key, values)
    
    return reduced


def print_results(results: dict, title: str = "Results", limit: int = None):
    """Prints results in a readable format."""
    print(f"\n{'='*50}")
    print(f"{title}")
    print(f"{'='*50}")
    
    items = sorted(results.items(), key=lambda x: x[1], reverse=True)
    if limit:
        items = items[:limit]
    
    for key, value in items:
        print(f"{key}: {value}")
    print()
