package com.freitas.hadoop;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

public class RunUtilities {
	

	public static <T> Class<T> tryToLoadClass(String jarPath, String className) throws MalformedURLException,
	ClassNotFoundException, IllegalAccessException, InstantiationException {

		if (jarPath != null) {
			return RunUtilities.<T> loadClassFromJar(jarPath.toString(), className);
		} else {
			return RunUtilities.<T> getClassFromName(className);
		}
	}
	

	@SuppressWarnings("unchecked")
	public static <T> Class<T> getClassFromName(String className) throws ClassNotFoundException,
			InstantiationException, IllegalAccessException {

		Class<T> jeClass;
		Class<?> c = Class.forName(className);
		jeClass = (Class<T>) c.newInstance();
		return jeClass;
	}
	
	@SuppressWarnings("rawtypes")
	public static Class basicGetClassFromName(String className) throws ClassNotFoundException,
			InstantiationException, IllegalAccessException {
		return Class.forName(className);
	}

	@SuppressWarnings("unchecked")
	public static <T> Class<T> loadClassFromJar(String jarPath, String className) throws ClassNotFoundException, MalformedURLException {
		
		ClassLoader prevCl = Thread.currentThread().getContextClassLoader();
		URL[] urls = { new URL("jar:" + jarPath + "!/") };
		ClassLoader cl = URLClassLoader.newInstance(urls, prevCl);
		Class<T> c = (Class<T>) cl.loadClass(className);
		return c;
	}
	
}
