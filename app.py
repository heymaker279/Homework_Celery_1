import smtplib
import time
from celery import Celery
from celery.result import AsyncResult
from flask import Flask, jsonify, request
from sqlalchemy import create_engine, Column, Integer, String, DateTime, func, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from flask.views import MethodView
from dotenv import dotenv_values

app_name = 'app'
env = dotenv_values(".env")
app = Flask(app_name)
app.config['JSON_AS_ASCII'] = False
app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/1'
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/2'
app.debug = True
engine = create_engine(
    f'postgresql://{env["DB_USER"]}:{env["DB_PASSWORD"]}@{env["DB_HOST"]}:{env["DB_PORT"]}/{env["DB_NAME"]}')
Base = declarative_base()
Session = sessionmaker(bind=engine)


# celery_app = Celery(app.name, backend=app.config['CELERY_RESULT_BACKEND'], broker=app.config['CELERY_BROKER_URL'])


def make_celery(app):
    app = app
    celery = Celery(app_name, backend=app.config['CELERY_RESULT_BACKEND'], broker=app.config['CELERY_BROKER_URL'])
    celery.conf.update(app.config)
    TaskBase = celery.Task
    class ContextTask(TaskBase):
        abstract = True
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)
    celery.Task = ContextTask
    return celery


celery_app = make_celery(app)


@celery_app.task()
def send_email(email_list):
    sender = env['EMAIL_NAME']
    password = env['EMAIL_PASSWORD']
    server = smtplib.SMTP(env['EMAIL_SMTP'], int(env['EMAIL_PORT']))
    server.set_debuglevel(1)
    server.starttls()
    message = "Hello, pythonic world!"
    try:
        server.ehlo(sender)
        server.login(sender, password)
        server.auth_plain()
        for email in email_list:
            print(email)
            server.sendmail(sender, email, message)
        server.quit()
        return 'The message was sent successfully!'
    except Exception as ex:
        return jsonify({'message': f"{ex} check your login or password please!"})


class HttpError(Exception):
    def __init__(self, status_code, message):
        self.status_code = status_code
        self.message = message


@app.errorhandler(HttpError)
def http_error_handler(er: HttpError):
    response = jsonify({'status': 'error', 'message': er.message})
    response.status_code = er.status_code
    return response


class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    name = Column(String(60), nullable=False)
    email = Column(String(100), nullable=False, unique=True)
    password = Column(String(100))


class Advertisement(Base):
    __tablename__ = 'advertisements'
    id = Column(Integer, primary_key=True)
    header = Column(String(64), nullable=False)
    description = Column(String, nullable=False)
    registration_time = Column(DateTime, server_default=func.now())
    owner = Column(ForeignKey(User.id))


Base.metadata.create_all(engine)


def get_email_list():
    with Session() as session:
        users_list = session.query(User).all()
        email_list = []
        for user in users_list:
            email_list.append(user.email)
        return email_list


print(get_email_list())


def get_item(session, item_id, cls):
    response = session.query(cls).get(item_id)
    if response is None:
        raise HttpError(404, f'{cls.__name__} does not exist')
    return response


class MailSend(MethodView):

    def get(self, task_id):
        global task
        status = "PENDING"
        while status == "PENDING":
            time.sleep(1)
            task = AsyncResult(task_id, app=celery_app)
            print(task.status)
            status = task.status
        return jsonify({
            'status': task.status,
            'result': task.get()
        })

    def post(self):
        # send_email('heymaker@yandex.ru')
        task = send_email.delay(get_email_list())
        return jsonify(
            {'task_id': task.id,
             'message': 'success'}
        )


class AdvView(MethodView):

    def get(self, adv_id):
        with Session() as session:
            adv = get_item(session, adv_id, Advertisement)
            return jsonify({
                'header': adv.header,
                'registration_time': adv.registration_time.isoformat(),
                'description': adv.description,
                'owner': adv.owner
            })

    def post(self):
        adv_data = request.json
        with Session() as session:
            new_adv = Advertisement(header=adv_data['header'], description=adv_data['description'],
                                    owner=adv_data['owner'])
            session.add(new_adv)
            session.commit()
            return jsonify({'status': 'ok', 'id': new_adv.id})

    def patch(self, adv_id):
        adv_data = request.json
        with Session() as session:
            adv = get_item(session, adv_id, Advertisement)
            for key, value in adv_data.items():
                setattr(adv, key, value)
            session.commit()
        return jsonify({'status': 'ok'})

    def delete(self, adv_id):
        with Session() as session:
            adv = get_item(session, adv_id, Advertisement)
            session.delete(adv)
            session.commit()
        return jsonify({'status': 'Ok'})


class UserView(MethodView):

    def get(self, user_id):
        with Session() as session:
            user = get_item(session, user_id, User)
            return jsonify({
                'name': user.name,
                'email': user.email,
                'password': user.password,
            })

    def post(self):
        user_data = request.json
        with Session() as session:
            new_user = User(name=user_data['name'], email=user_data['email'], password=user_data['password'])
            session.add(new_user)
            session.commit()
            return jsonify({'status': 'ok', 'id': new_user.id})

    def patch(self, user_id):
        user_data = request.json
        with Session() as session:
            user = get_item(session, user_id, User)
            for key, value in user_data.items():
                setattr(user, key, value)
            session.commit()
        return jsonify({'status': 'ok'})

    def delete(self, user_id):
        with Session() as session:
            user = get_item(session, user_id, User)
            session.delete(user)
            session.commit()
        return jsonify({'status': 'Ok'})


app.add_url_rule('/adv/<int:adv_id>', view_func=AdvView.as_view('adv_get'), methods=['GET', 'PATCH', 'DELETE'])
app.add_url_rule('/adv', view_func=AdvView.as_view('adv_post'), methods=['POST'])
app.add_url_rule('/user/<int:user_id>', view_func=UserView.as_view('user_get'), methods=['GET', 'PATCH', 'DELETE'])
app.add_url_rule('/user', view_func=UserView.as_view('user_post'), methods=['POST'])
app.add_url_rule('/email_send', view_func=MailSend.as_view('mail_send'), methods=['POST'])
app.add_url_rule(f"/email/<task_id>", view_func=MailSend.as_view('email'), methods=['GET'])

if __name__ == '__main__':
    app.run()
