from pypescript.utils import setup_logging, BaseClass


def test_base_class():
    self = BaseClass()
    self.log_info("I'm the BaseClass.")


if __name__ == '__main__':
    setup_logging()
    test_base_class()
