from app.cli import print_welcome


def test_print_welcome(capsys):
    print_welcome()
    captured = capsys.readouterr()

    assert "Welcome to SheetChat" in captured.out
    assert "Type a SQL SELECT query" in captured.out