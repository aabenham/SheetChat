from app.cli import print_welcome


def test_print_welcome(capsys):
    print_welcome()
    captured = capsys.readouterr()

    assert "Welcome to SheetChat" in captured.out
    assert "load <csv_path> <table_name>" in captured.out
    assert "tables" in captured.out
    assert "schema <table>" in captured.out
    assert "exit" in captured.out