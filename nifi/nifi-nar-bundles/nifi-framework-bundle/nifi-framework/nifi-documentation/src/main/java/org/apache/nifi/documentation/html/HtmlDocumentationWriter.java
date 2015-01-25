/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.documentation.html;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.documentation.DocumentationWriter;
import org.apache.nifi.processor.annotation.CapabilityDescription;
import org.apache.nifi.processor.annotation.Tags;

public class HtmlDocumentationWriter implements DocumentationWriter {

	private static final String apacheLicense;

	static {
		String value = null;
		try {
			value = IOUtils.toString(ClassLoader.getSystemResourceAsStream("apache.license"));
		} catch (IOException e) {
			e.printStackTrace();
		}

		apacheLicense = value;
	}

	@Override
	public void write(final ConfigurableComponent configurableComponent, final OutputStream streamToWriteTo,
			final boolean includesAdditionalDocumentation) throws IOException {

		try {
			XMLStreamWriter xmlStreamWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(streamToWriteTo,
					"UTF-8");
			xmlStreamWriter.writeDTD("<!DOCTYPE html>");
			xmlStreamWriter.writeStartElement("html");
			xmlStreamWriter.writeAttribute("lang", "en");
			writeHead(configurableComponent, xmlStreamWriter);
			writeBody(configurableComponent, xmlStreamWriter, includesAdditionalDocumentation);
			xmlStreamWriter.writeEndElement();
			xmlStreamWriter.close();
		} catch (XMLStreamException | FactoryConfigurationError e) {
			throw new IOException("Unable to create XMLOutputStream", e);
		}
	}

	protected final static void writeSimpleElement(XMLStreamWriter writer, String elementName, String elementValue,
			boolean strong) throws XMLStreamException {
		writer.writeStartElement(elementName);
		if (strong) {
			writer.writeStartElement("strong");
		}
		writer.writeCharacters(elementValue);
		if (strong) {
			writer.writeEndElement();
		}
		writer.writeEndElement();
	}

	protected final static void writeSimpleElement(XMLStreamWriter writer, String elementName, String elementValue)
			throws XMLStreamException {
		writeSimpleElement(writer, elementName, elementValue, false);
	}

	protected void writeHead(final ConfigurableComponent configurableComponent, XMLStreamWriter xmlStreamWriter)
			throws XMLStreamException {

		// write the apache license
		xmlStreamWriter.writeComment(apacheLicense);
		xmlStreamWriter.writeStartElement("head");
		xmlStreamWriter.writeStartElement("meta");
		xmlStreamWriter.writeAttribute("charset", "utf-8");
		xmlStreamWriter.writeEndElement();
		writeSimpleElement(xmlStreamWriter, "title", getTitle(configurableComponent));

		xmlStreamWriter.writeStartElement("link");
		xmlStreamWriter.writeAttribute("rel", "stylesheet");
		xmlStreamWriter.writeAttribute("href", "../../css/component-usage.css");
		xmlStreamWriter.writeAttribute("type", "text/css");
		xmlStreamWriter.writeEndElement();

		xmlStreamWriter.writeEndElement();
	}

	protected String getTitle(final ConfigurableComponent configurableComponent) {
		return configurableComponent.getClass().getSimpleName();
	}

	private final void writeBody(final ConfigurableComponent configurableComponent,
			final XMLStreamWriter xmlStreamWriter, final boolean hasAdditionalDetails) throws XMLStreamException {
		xmlStreamWriter.writeStartElement("body");
		writeDescription(configurableComponent, xmlStreamWriter, hasAdditionalDetails);
		writeTags(configurableComponent, xmlStreamWriter);
		writeProperties(configurableComponent, xmlStreamWriter);
		writeBodySub(configurableComponent, xmlStreamWriter);
		xmlStreamWriter.writeEndElement();
	}

	protected void writeBodySub(final ConfigurableComponent configurableComponent, final XMLStreamWriter xmlStreamWriter)
			throws XMLStreamException {
	}

	private void writeTags(final ConfigurableComponent configurableComponent, final XMLStreamWriter xmlStreamWriter)
			throws XMLStreamException {
		final Tags tags = configurableComponent.getClass().getAnnotation(Tags.class);
		xmlStreamWriter.writeStartElement("p");
		if (tags != null) {
			final String tagString = StringUtils.join(tags.value(), ", ");
			xmlStreamWriter.writeCharacters("Tags: ");
			xmlStreamWriter.writeCharacters(tagString);
		} else {
			xmlStreamWriter.writeCharacters("No Tags provided.");
		}
		xmlStreamWriter.writeEndElement();

	}

	protected void writeDescription(final ConfigurableComponent configurableComponent,
			final XMLStreamWriter xmlStreamWriter, final boolean hasAdditionalDetails) throws XMLStreamException {
		writeSimpleElement(xmlStreamWriter, "h2", "Description: ");
		writeSimpleElement(xmlStreamWriter, "p", getDescription(configurableComponent));
		if (hasAdditionalDetails) {
			xmlStreamWriter.writeStartElement("p");

			writeLink(xmlStreamWriter, "Additional Details...", "additionalDetails.html");

			xmlStreamWriter.writeEndElement();
		}
	}

	private void writeLink(final XMLStreamWriter xmlStreamWriter, final String text, final String location)
			throws XMLStreamException {
		xmlStreamWriter.writeStartElement("a");
		xmlStreamWriter.writeAttribute("href", location);
		xmlStreamWriter.writeCharacters(text);
		xmlStreamWriter.writeEndElement();
	}

	protected String getDescription(final ConfigurableComponent configurableComponent) {
		final CapabilityDescription capabilityDescription = configurableComponent.getClass().getAnnotation(
				CapabilityDescription.class);

		final String description;
		if (capabilityDescription != null) {
			description = capabilityDescription.value();
		} else {
			description = "No description provided.";
		}

		return description;
	}

	protected void writeProperties(final ConfigurableComponent configurableComponent,
			final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
		xmlStreamWriter.writeStartElement("p");
		writeSimpleElement(xmlStreamWriter, "strong", "Properties: ");
		xmlStreamWriter.writeEndElement();
		xmlStreamWriter.writeStartElement("p");
		xmlStreamWriter.writeCharacters("In the list below, the names of required properties appear in ");
		writeSimpleElement(xmlStreamWriter, "strong", "bold");
		xmlStreamWriter.writeCharacters(". Any"
				+ "other properties (not in bold) are considered optional. The table also "
				+ "indicates any default values, whether a property supports the ");
		writeLink(xmlStreamWriter, "NiFi Expression Language (or simply EL)",
				"../../html/expression-language-guide.html");
		xmlStreamWriter.writeCharacters(", and whether a property is considered "
				+ "\"sensitive\", meaning that its value will be encrypted. Before entering a "
				+ "value in a sensitive property, ensure that the ");
		writeSimpleElement(xmlStreamWriter, "strong", "nifi.properties");
		xmlStreamWriter.writeCharacters(" file has " + "an entry for the property ");
		writeSimpleElement(xmlStreamWriter, "strong", "nifi.sensitive.props.key");
		xmlStreamWriter.writeCharacters(".");
		xmlStreamWriter.writeEndElement();

		List<PropertyDescriptor> properties = configurableComponent.getPropertyDescriptors();
		if (properties.size() > 0) {
			xmlStreamWriter.writeStartElement("table");

			xmlStreamWriter.writeStartElement("tr");
			writeSimpleElement(xmlStreamWriter, "th", "Name");
			writeSimpleElement(xmlStreamWriter, "th", "Description");
			writeSimpleElement(xmlStreamWriter, "th", "Default Value");
			writeSimpleElement(xmlStreamWriter, "th", "Valid Values");
			xmlStreamWriter.writeStartElement("th");
			writeLink(xmlStreamWriter, "EL", "../../html/expression-language-guide.html");
			xmlStreamWriter.writeEndElement();
			writeSimpleElement(xmlStreamWriter, "th", "Sensitive");
			xmlStreamWriter.writeEndElement();

			for (PropertyDescriptor property : properties) {
				xmlStreamWriter.writeStartElement("tr");
				writeSimpleElement(xmlStreamWriter, "td", property.getName(), property.isRequired());
				writeSimpleElement(xmlStreamWriter, "td", property.getDescription());
				writeSimpleElement(xmlStreamWriter, "td", property.getDefaultValue());
				writeValidValues(xmlStreamWriter, property);
				writeSimpleElement(xmlStreamWriter, "td", property.isExpressionLanguageSupported() ? "Yes" : "No");
				writeSimpleElement(xmlStreamWriter, "td", property.isSensitive() ? "Yes" : "No");
				xmlStreamWriter.writeEndElement();
			}

			// TODO support dynamic properties...
			xmlStreamWriter.writeEndElement();

		} else {
			writeSimpleElement(xmlStreamWriter, "p", "This component has no required or optional properties.");
		}
	}

	protected void writeValidValues(XMLStreamWriter xmlStreamWriter, PropertyDescriptor property)
			throws XMLStreamException {
		xmlStreamWriter.writeStartElement("td");
		if (property.getAllowableValues() != null && property.getAllowableValues().size() > 0) {
			xmlStreamWriter.writeStartElement("ul");
			for (AllowableValue value : property.getAllowableValues()) {
				writeSimpleElement(xmlStreamWriter, "li", value.getValue());
			}
			xmlStreamWriter.writeEndElement();
		}
		xmlStreamWriter.writeEndElement();
	}
}
