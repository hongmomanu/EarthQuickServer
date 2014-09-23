package lumprj.java.eqim;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.lang.Exception;
import java.lang.System;

import cn.gd.seismology.config.XmlLocation;
import cn.gd.seismology.config.XmlLocation.SourceParam;
import cn.gd.seismology.config.XmlLocation.Network;
import cn.gd.seismology.liss.client.LissClient;
import cn.gd.seismology.liss.client.LissException;
import cn.gd.seismology.liss.message.Result;
import clojure.lang.RT;
import clojure.lang.Var;



public class EqimConnectorTip {

    private LissClient client = null;

    private InputStream locInputStream = null;

    private boolean connectionOK = false;

    private boolean bQuit = false;

    private Thread clientThread;
    private String ip="";
    private int port=5001;
    private  String user="";
    private String pass="";

    public EqimConnectorTip(String ip,int port,String user,String pass){
        this.ip=ip;
        this.port=port;
        this.user=user;
        this.pass=pass;
    }

    public void connectServer() throws IOException, LissException {




        //client = new LissClient("10.33.5.5", 5001);  //ip 端口
        client = new LissClient(this.ip, this.port);  //ip 端口
        client.login(this.user,this.pass);//user password
        locInputStream = client.retrieveResult("LOC");
        //RT.loadResourceScript("lumprj/controller/realstream.clj");
        //Var foo = RT.var("lumprj.controller.realstream", "java-clojure-test");
        //Object result = foo.invoke("121");



    }

    public void quit() {
        try {
            client.quit();
        } catch (Exception ex) {
            // ex.printStackTrace();
        }
        this.bQuit = true;
    }



    public void loopConnect() {
        while (!bQuit) {
            try {
                connectServer();
                System.out.println("connect ok,"+this.ip+":"+this.port);
                connectionOK = true;
                break;
            } catch (Exception ex) {
                connectionOK = false;

                try {
                    Thread.sleep(10 * 1000);
                } catch (InterruptedException iEx) {
                }
            }finally {
                //System.out.println("connecting try,"+this.ip+":"+this.port);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void receiveAndPublish() {
        System.out.println(this.ip +","+this.user+","+this.pass);
        loopConnect();
        clientThread = new Thread("EqimMonitorDispatcher") {

            public void run() {
                System.out.println(Thread.currentThread().getName()+  " receiverAndPublish...");
                Result res = null;
                while (!bQuit) {
                    try {
                        res = null;

                        res = Result.buildResult(locInputStream);
                        if (res != null) {
                            parse(res);
                            System.out.println("receive a message, to do something ...");
                        } else
                            loopConnect();

                    } catch (InterruptedIOException iioEx) {

                        loopConnect();
                    } catch (IOException ioEx) {
                        ioEx.printStackTrace();

                        loopConnect();
                    }
                }
            }


        };
        clientThread.start();
    }
    public void clojureeqm (SourceParam sp ,Network net){

       /* RT.loadResourceScript("lumprj/controller/realstream.clj");
        Var foo = RT.var("lumprj.controller.realstream", "send-eqim-info");
        Object result = foo.invoke(sp,net);*/
    }
    private void parse(Result res) {
        XmlLocation xl = new XmlLocation();
        xl.parseLocation(new ByteArrayInputStream(res.getData()));
        SourceParam sp = xl.getSource();

        Network net =xl.getNetwork();
        //sp.Location_cname
        try {
           this.clojureeqm(sp,net);
        }catch (Exception e){
            System.out.println(e.getMessage());
        }


    }

    public static void main(String[] args){
        EqimConnectorTip t=new EqimConnectorTip("10.33.8.174",5001,"show","show");
        t.receiveAndPublish();
    }

}
