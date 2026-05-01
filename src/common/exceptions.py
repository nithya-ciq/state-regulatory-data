"""Custom exception hierarchy for the jurisdiction taxonomy pipeline."""


class JurisdictionError(Exception):
    """Base exception for all jurisdiction pipeline errors."""

    pass


class ConfigurationError(JurisdictionError):
    """Raised when configuration is invalid or missing."""

    pass


class DataAcquisitionError(JurisdictionError):
    """Raised when Census data download or parsing fails."""

    pass


class ClassificationError(JurisdictionError):
    """Raised when state classification data is invalid or incomplete."""

    pass


class ValidationError(JurisdictionError):
    """Raised when data validation checks fail."""

    pass


class PipelineError(JurisdictionError):
    """Raised when a pipeline phase fails."""

    def __init__(self, phase: str, message: str) -> None:
        self.phase = phase
        super().__init__(f"Pipeline phase '{phase}' failed: {message}")


class ResearchError(JurisdictionError):
    """Raised when the state research tool encounters an error."""

    pass
