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
package org.apache.nifi.xml;

import org.apache.nifi.properties.SensitivePropertyProvider;
import org.apache.nifi.properties.SensitivePropertyProviderFactory;
import org.apache.nifi.properties.scheme.ProtectionScheme;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Objects;

public abstract class XmlCryptoParser {

    protected static final String ENCRYPTION_ATTRIBUTE_NAME = "encryption";
    protected static final String PARENT_IDENTIFIER = "identifier";
    protected static final String PROPERTY_ELEMENT = "property";

    protected final SensitivePropertyProvider cryptoProvider;
    protected SensitivePropertyProviderFactory providerFactory;

    public XmlCryptoParser(final SensitivePropertyProviderFactory providerFactory, final ProtectionScheme scheme) {
        this.providerFactory = providerFactory;
        cryptoProvider = providerFactory.getProvider(scheme);
    }

    protected void cryptographicXmlOperation(final InputStream encryptedXmlContent, final OutputStream decryptedOutputStream) {
        final XMLOutputFactory factory = XMLOutputFactory.newInstance();
        factory.setProperty("com.ctc.wstx.outputValidateStructure", false);

        try {
            final XMLEventReader eventReader = getXMLReader(encryptedXmlContent);
            final XMLEventWriter xmlWriter = factory.createXMLEventWriter(decryptedOutputStream);
            String groupIdentifier = "";

            while(eventReader.hasNext()) {
                XMLEvent event = eventReader.nextEvent();

                if (isGroupIdentifier(event)) {
                    groupIdentifier = getGroupIdentifier(eventReader.nextEvent());
                }

                if (isSensitiveElement(event)) {
                    xmlWriter.add(updateStartElementEncryptionAttribute(event));
                    xmlWriter.add(cryptoOperationOnCharacters(eventReader.nextEvent(), groupIdentifier, getPropertyName(event)));
                } else {
                    try {
                        xmlWriter.add(event);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed operation on XML content", e);
                    }
                }
            }

            eventReader.close();
            xmlWriter.flush();
            xmlWriter.close();
        } catch (Exception e) {
            throw new RuntimeException("Failed operation on XML content", e);
        }
    }

    /**
     * Update the StartElement 'encryption' attribute for a sensitive value to add or remove the respective encryption details eg. encryption="aes/gcm/128"
     * @param xmlEvent A 'sensitive' StartElement that contains the 'encryption' tag attribute
     * @return The updated StartElement
     */
    protected abstract StartElement updateStartElementEncryptionAttribute(final XMLEvent xmlEvent);

    /**
     * Perform an encrypt or decrypt cryptographic operation on a Characters element
     * @param xmlEvent A Characters XmlEvent
     * @param groupIdentifier The XML <identifier/> tag
     * @return The Characters XmlEvent that has been updated by the cryptographic operation
     */
    protected abstract Characters cryptoOperationOnCharacters(final XMLEvent xmlEvent, final String groupIdentifier, final String propertyName);

    private String getGroupIdentifier(final XMLEvent xmlEvent) {
        if (xmlEvent.isCharacters()) {
            return xmlEvent.asCharacters().getData();
        } else {
            return "";
        }
    }

    protected String getPropertyName(final XMLEvent xmlEvent) {
        return xmlEvent.asStartElement().getName().toString();
    }

    protected boolean isGroupIdentifier(final XMLEvent xmlEvent) {
        return xmlEvent.isStartElement()
                && xmlEvent.asStartElement().getName().toString().equals(PARENT_IDENTIFIER);
    }

    protected boolean isSensitiveElement(final XMLEvent xmlEvent) {
        return  xmlEvent.isStartElement()
                && xmlEvent.asStartElement().getName().toString().equals(PROPERTY_ELEMENT)
                && elementHasEncryptionAttribute(xmlEvent.asStartElement());
    }

    protected XMLEventReader getXMLReader(final InputStream fileStream) throws XMLStreamException, FileNotFoundException {
        XMLInputFactory xmlInputFactory = XMLInputFactory.newFactory();
        xmlInputFactory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
        xmlInputFactory.setProperty(XMLInputFactory.SUPPORT_DTD, false);

        return xmlInputFactory.createXMLEventReader(fileStream);
    }

    private boolean elementHasEncryptionAttribute(final StartElement xmlEvent) {
        return xmlElementHasAttribute(xmlEvent, ENCRYPTION_ATTRIBUTE_NAME);
    }

    private boolean xmlElementHasAttribute(final StartElement xmlEvent, final String attributeName) {
        return !Objects.isNull(xmlEvent.getAttributeByName(new QName(attributeName)));
    }

    protected StartElement updateElementAttribute(final XMLEvent xmlEvent, final String attributeName, final String attributeValue) {
        final XMLEventFactory eventFactory = XMLEventFactory.newInstance();
        StartElement encryptedElement = xmlEvent.asStartElement();

        Iterator<Attribute> currentAttributes = encryptedElement.getAttributes();
        ArrayList<Attribute> updatedAttributes = new ArrayList<>();

        while (currentAttributes.hasNext()) {
            final Attribute attribute = currentAttributes.next();
            if (attribute.getName().equals(new QName(attributeName))) {
                updatedAttributes.add(eventFactory.createAttribute(attribute.getName(), attributeValue));
            } else {
                updatedAttributes.add(attribute);
            }
        }

        return eventFactory.createStartElement(encryptedElement.getName(), updatedAttributes.iterator(), encryptedElement.getNamespaces());
    }
}
