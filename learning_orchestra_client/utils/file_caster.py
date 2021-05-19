from os import PathLike


def python_file_caster(source: str, filename: str = '<string>',
                       flags: int = 0,
                       dont_inherit: bool = False,
                       optimize: int = -1) -> str:
    """
    description: Convert source into a string to later be executed by an
     exec() statement.

    :param source: The argument param should be a valid filepath of the
    source file.
    :param filename: The filename argument should give the file from which the
    code was read;
    :param flags: The optional argument flags control which future statements
    affect the compilation of source.
    :param dont_inherit: The optional argument dont_inherit control which future
    statements affect the compilation of source.
    :param optimize: The argument optimize specifies the optimization level of
    the compiler.

    :raise OSError: invalid file path was passed to source
    :raise SyntaxError: Syntax error was found on the source file
    :raise ValueError: Value error was found on the source file

    :returns str: string value of source path
    """
    file_as_string = ''
    file = None
    try:
        file = open(source)
        file_as_string = file.read()
        compile(source=file_as_string,
                filename=filename,
                mode='exec',
                flags=flags,
                dont_inherit=dont_inherit,
                optimize=optimize)

    except OSError:
        print('Not valid filepath')
        raise OSError('Not a valid filepath')
    except SyntaxError as synError:
        print('Syntax error in current file')
        raise SyntaxError(synError)
    except ValueError:
        print('Value error in current file')
    finally:
        file.close()

    return file_as_string
