package cn.org.gddsn.jopens.pod.amq;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Date;
import java.util.Enumeration;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import clojure.lang.RT;
import clojure.lang.Var;
import lumprj.java.AmqClojure;

//import cn.org.gddsn.jopens.pod.bean.MessageBean;
//import cn.org.gddsn.jopens.pod.util.PodUtil;

public class AmqEarService implements MessageListener {

    static Logger logger = Logger.getLogger(AmqEarService.class);
    public static String cfgFile = "/home/jack/soft/lumprj/src/lumprj/java/applicationContext-amqEar-jms.xml";
    static XmlBeanFactory ac;
    private JmsTemplate jmsTemplate;
    private AmqClojure amqclj;

    private DefaultMessageListenerContainer container;

    public AmqEarService() {
    }

    public void runListening() {
        container.start();
        logger.info("Ready Receiving...");

    }

    public void send(final byte[] bytes) {
        jmsTemplate.send(new MessageCreator() {
            public Message createMessage(Session session) throws JMSException {
                BytesMessage bmsg = session.createBytesMessage();
                bmsg.writeBytes(bytes);
                session.close();
                return bmsg;
            }
        });

    }

    public void setclojure (AmqClojure acl){
        logger.info("acl change begin");
        this.amqclj=acl;
        logger.info("acl change end");

    }
    public void clojureRts(String mess){
        this.amqclj.amqplay(mess);

    }
    public void onMessage(Message message) {
        //logger.info("get a message at time: " + PodUtil.dft.format(new Date()));
        if (message instanceof TextMessage) {
            TextMessage tm = (TextMessage) message;

            String mess = null;

            try {

                logger.info("JMSDestination: "
                        + tm.getJMSDestination().toString());
                logger.info("getPropertyNames:"
                        + tm.getPropertyNames().hasMoreElements());
                Enumeration e = tm.getPropertyNames();
                while (e.hasMoreElements()) {
                    logger.info(e.nextElement() + "");
                }

                mess = tm.getText();
                logger.info("receive textMessages: eventId=" + mess);

                this.clojureRts(mess);
                /*RT.loadResourceScript("lumprj/controller/realstream.clj");
                Var foo = RT.var("lumprj.controller.realstream", "send-rts-info");
                Object result = foo.invoke(mess);
                logger.info("receive over");*/

                //MessageBean mssageBean = PodUtil.String2MessageBean(mess);
                /*if (mssageBean == null) {
                    logger.info("receive textMessage: eventId=" + mess);

                } else {
                    //logger.info("receive messageBean: " + mssageBean);
                }*/
            } catch (Exception e) {
                logger.info(e.getMessage()+"receive error");
                e.printStackTrace();
            }

        }
        if (message instanceof BytesMessage) {
            BytesMessage bmsg = (BytesMessage) message;
            try {
                String destName = bmsg.getJMSDestination().toString();
                logger.info("destinationName: " + destName);
            } catch (JMSException e) {
                e.printStackTrace();
            }

        }
    }

    public void setJmsTemplate(JmsTemplate jmsTemplate) {
        this.jmsTemplate = jmsTemplate;
    }

    public DefaultMessageListenerContainer getContainer() {
        return container;
    }

    public void setContainer(DefaultMessageListenerContainer container) {
        this.container = container;
    }

    public static void main(String[] args) {
        //PodUtil.loadLog4jConfig();

        logger.info("load " + cfgFile);
        Resource res = new FileSystemResource(cfgFile);
        ac = new XmlBeanFactory(res);
        AmqEarService amq = (AmqEarService) ac.getBean("amqEarService");
        amq.runListening();

    }

}
