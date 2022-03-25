# TODO add exception handling
class JATSContribException(Exception):
    pass


class MissingAuthorsException(Exception):
    pass


class MissingDoiException(Exception):
    pass


class MissingTitleException(Exception):
    pass


class NoSchemaException(Exception):
    pass


class UnicodeHandlerError(Exception):
    """
    Error in the UnicodeHandler.
    """

    pass


class UnparseableException(Exception):
    pass


class WrongFormatException(Exception):
    pass


class WrongSchemaException(Exception):
    pass
