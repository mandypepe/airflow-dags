from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint
from flask_admin import BaseView, expose


class TestView(BaseView):
    @expose('/')
    def test(self):
        data = [{'column_a': 'Content',
                 'column_b': '123',
                 'column_c': 'Test'}]

        return self.render("test.html", data=data)


admin_view_ = TestView(category="Test Plugin", name="Test View")

blue_print_ = Blueprint("test_plugin",
                        __name__,
                        template_folder='templates',
                        static_folder='static',
                        static_url_path='/static/test_plugin')


class AirflowTestPlugin(AirflowPlugin):
    name = "test_plugin"
    admin_views = [admin_view_]
    flask_blueprints = [blue_print_]
