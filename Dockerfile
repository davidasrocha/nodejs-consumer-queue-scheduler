FROM rabbitmq:3-management

# install Event Exchange Plugin
# https://www.rabbitmq.com/event-exchange.html
RUN rabbitmq-plugins enable rabbitmq_event_exchange