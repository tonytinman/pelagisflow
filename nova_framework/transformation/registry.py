"""
Transformation registry for managing and discovering transformations.

The registry maintains a catalog of available transformations with metadata
including type, version, dependencies, and execution requirements.
"""

import json
import yaml
from pathlib import Path
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict

from nova_framework.transformation.base import TransformationType


@dataclass
class TransformationMetadata:
    """Metadata for a registered transformation."""
    name: str
    type: TransformationType
    version: str
    description: str
    module_path: Optional[str] = None  # For Python transformations
    class_name: Optional[str] = None   # For Scala transformations
    jar_path: Optional[str] = None     # For Scala transformations
    function_name: str = "transform"   # For Python transformations
    dependencies: List[str] = None     # List of required packages/JARs
    author: Optional[str] = None
    tags: List[str] = None
    config_schema: Optional[Dict[str, Any]] = None  # JSON schema for config validation

    def __post_init__(self):
        """Initialize default values for lists."""
        if self.dependencies is None:
            self.dependencies = []
        if self.tags is None:
            self.tags = []

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary, handling enum serialization."""
        result = asdict(self)
        result['type'] = self.type.value
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TransformationMetadata':
        """Create from dictionary, handling enum deserialization."""
        if 'type' in data and isinstance(data['type'], str):
            data['type'] = TransformationType(data['type'])
        return cls(**data)


class TransformationRegistry:
    """
    Registry for managing transformations.

    The registry stores metadata about available transformations and provides
    discovery and lookup capabilities. Transformations are registered via:
    1. YAML/JSON configuration files in transformations/registry/
    2. Programmatic registration via register() method
    """

    def __init__(self, registry_path: Optional[Path] = None):
        """
        Initialize the transformation registry.

        Args:
            registry_path: Path to registry directory (default: transformations/registry/)
        """
        if registry_path is None:
            registry_path = Path(__file__).parent.parent / "transformations" / "registry"

        self.registry_path = Path(registry_path)
        self._transformations: Dict[str, TransformationMetadata] = {}
        self._load_registry()

    def _load_registry(self) -> None:
        """Load all transformations from registry files."""
        if not self.registry_path.exists():
            self.registry_path.mkdir(parents=True, exist_ok=True)
            return

        # Load all YAML and JSON files
        for file_path in self.registry_path.glob("*.yaml"):
            self._load_registry_file(file_path)

        for file_path in self.registry_path.glob("*.yml"):
            self._load_registry_file(file_path)

        for file_path in self.registry_path.glob("*.json"):
            self._load_registry_file(file_path)

    def _load_registry_file(self, file_path: Path) -> None:
        """
        Load transformations from a single registry file.

        Args:
            file_path: Path to registry file
        """
        try:
            with open(file_path, 'r') as f:
                if file_path.suffix in ['.yaml', '.yml']:
                    data = yaml.safe_load(f)
                else:
                    data = json.load(f)

            # Handle both single transformation and list of transformations
            transformations = data if isinstance(data, list) else [data]

            for transform_data in transformations:
                metadata = TransformationMetadata.from_dict(transform_data)
                self._transformations[metadata.name] = metadata

        except Exception as e:
            print(f"Warning: Failed to load registry file {file_path}: {e}")

    def register(self, metadata: TransformationMetadata) -> None:
        """
        Register a transformation programmatically.

        Args:
            metadata: Transformation metadata

        Raises:
            ValueError: If transformation with same name already exists
        """
        if metadata.name in self._transformations:
            raise ValueError(
                f"Transformation '{metadata.name}' is already registered. "
                f"Use update() to modify existing registration."
            )

        self._transformations[metadata.name] = metadata

    def update(self, metadata: TransformationMetadata) -> None:
        """
        Update an existing transformation registration.

        Args:
            metadata: Updated transformation metadata
        """
        self._transformations[metadata.name] = metadata

    def get(self, name: str) -> Optional[TransformationMetadata]:
        """
        Get transformation metadata by name.

        Args:
            name: Transformation name

        Returns:
            TransformationMetadata if found, None otherwise
        """
        return self._transformations.get(name)

    def list(
        self,
        type_filter: Optional[TransformationType] = None,
        tags: Optional[List[str]] = None
    ) -> List[TransformationMetadata]:
        """
        List all registered transformations with optional filtering.

        Args:
            type_filter: Filter by transformation type
            tags: Filter by tags (returns transformations with ANY of the tags)

        Returns:
            List of TransformationMetadata
        """
        results = list(self._transformations.values())

        if type_filter:
            results = [t for t in results if t.type == type_filter]

        if tags:
            results = [t for t in results if any(tag in t.tags for tag in tags)]

        return results

    def exists(self, name: str) -> bool:
        """
        Check if transformation exists in registry.

        Args:
            name: Transformation name

        Returns:
            True if exists, False otherwise
        """
        return name in self._transformations

    def unregister(self, name: str) -> bool:
        """
        Remove transformation from registry.

        Args:
            name: Transformation name

        Returns:
            True if removed, False if not found
        """
        if name in self._transformations:
            del self._transformations[name]
            return True
        return False

    def save_to_file(self, file_path: Path) -> None:
        """
        Save all registered transformations to a file.

        Args:
            file_path: Path to save registry (YAML or JSON)
        """
        transformations_data = [t.to_dict() for t in self._transformations.values()]

        with open(file_path, 'w') as f:
            if file_path.suffix in ['.yaml', '.yml']:
                yaml.safe_dump(transformations_data, f, default_flow_style=False, sort_keys=False)
            else:
                json.dump(transformations_data, f, indent=2)

    def get_stats(self) -> Dict[str, Any]:
        """
        Get registry statistics.

        Returns:
            Dictionary with registry statistics
        """
        type_counts = {}
        for transform_type in TransformationType:
            type_counts[transform_type.value] = len([
                t for t in self._transformations.values() if t.type == transform_type
            ])

        return {
            'total_transformations': len(self._transformations),
            'by_type': type_counts,
            'transformations': list(self._transformations.keys())
        }
