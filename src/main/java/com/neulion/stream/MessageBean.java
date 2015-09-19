package com.neulion.stream;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MessageBean {

	private long msgID;
	private long bitrate;
	private String productID;
	private String clientID;
	private String viewID;
	private String appType;
	private String eventType;
	private String deviceType;
	
	public static MessageBean parse(String line) throws UnsupportedEncodingException {
		MessageBean bean = new MessageBean();
		line = URLDecoder.decode(line, "utf-8");
		try {
			bean.setBitrate(Long.parseLong(MessageBean.getParameter(line, "bitrate")));
		} catch (NumberFormatException e) {
			bean.setBitrate(0);
		}
		bean.setClientID(MessageBean.getParameter(line, "clientID"));
		try {
			bean.setMsgID(Long.parseLong(MessageBean.getParameter(line, "msgID")));
		} catch (NumberFormatException e) {
			bean.setMsgID(0);
		}
		bean.setProductID(MessageBean.getParameter(line, "productID"));
		bean.setViewID(MessageBean.getParameter(line, "viewID"));
		bean.setAppType(MessageBean.getParameter(line, "appType"));
		bean.setEventType(MessageBean.getParameter(line, "eventType"));
		bean.setDeviceType(MessageBean.getParameter(line, "deviceType"));
		return bean;
	}
	
	private static String getParameter(String line, String name) {
		String value = "";
		Pattern p = Pattern.compile(name + "=([^&]*)&");
		Matcher m = p.matcher(line);
		if (m.find()) {
			value = m.group(1);
		}
		return value;
	}
	
	public long getMsgID() {
		return msgID;
	}
	public void setMsgID(long msgID) {
		this.msgID = msgID;
	}
	public long getBitrate() {
		return bitrate;
	}
	public void setBitrate(long bitrate) {
		this.bitrate = bitrate;
	}
	public String getProductID() {
		return productID;
	}
	public void setProductID(String productID) {
		this.productID = productID;
	}
	public String getClientID() {
		return clientID;
	}
	public void setClientID(String clientID) {
		this.clientID = clientID;
	}
	public String getViewID() {
		return viewID;
	}
	public void setViewID(String viewID) {
		this.viewID = viewID;
	}
	public String getAppType() {
		return appType;
	}
	public void setAppType(String appType) {
		this.appType = appType;
	}
	public String getEventType() {
		return eventType;
	}
	public void setEventType(String eventType) {
		this.eventType = eventType;
	}

	public String getDeviceType() {
		return deviceType;
	}

	public void setDeviceType(String deviceType) {
		this.deviceType = deviceType;
	}
}
