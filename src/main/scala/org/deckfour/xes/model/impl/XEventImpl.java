/*
 * OpenXES
 * 
 * The reference implementation of the XES meta-model for event 
 * log data management.
 * 
 * Copyright (c) 2008 Christian W. Guenther (christian@deckfour.org)
 * 
 * 
 * LICENSE:
 * 
 * This code is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA
 * 
 * EXEMPTION:
 * 
 * The use of this software can also be conditionally licensed for
 * other programs, which do not satisfy the specified conditions. This
 * requires an exemption from the general license, which may be
 * granted on a per-case basis.
 * 
 * If you want to license the use of this software with a program
 * incompatible with the LGPL, please contact the author for an
 * exemption at the following email address: 
 * christian@deckfour.org
 * 
 */
package org.deckfour.xes.model.impl;

import java.util.Set;

import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.model.XAttributeMap;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.util.XAttributeUtils;

/**
 * Implementation for the XEvent interface.
 * 
 * @author Christian W. Guenther (christian@deckfour.org)
 *
 */
public class XEventImpl implements XEvent {
	
	/**
	 * Map of attributes for this event.
	 */
	private XAttributeMap attributes;
	
	/**
	 * Creates a new event.
	 */
	public XEventImpl() {
		this(new XAttributeMapImpl());
	}

	/**
	 * Creates a new event.
	 * 
	 * @param attributes Map of attribute for the event.
	 */
	public XEventImpl(XAttributeMap attributes) {
		this.attributes = attributes;
	}

	/* (non-Javadoc)
	 * @see org.deckfour.xes.model.XAttributable#getAttributes()
	 */
	public XAttributeMap getAttributes() {
		return attributes;
	}

	/* (non-Javadoc)
	 * @see org.deckfour.xes.model.XAttributable#setAttributes(java.util.Map)
	 */
	public void setAttributes(XAttributeMap attributes) {
		this.attributes = attributes;
	}

	/* (non-Javadoc)
	 * @see org.deckfour.xes.model.XAttributable#getExtensions()
	 */
	public Set<XExtension> getExtensions() {
		return XAttributeUtils.extractExtensions(attributes);
	}
	
	/**
	 * Clones this event, i.e. creates a deep copy.
	 */
	public Object clone() {
		XEventImpl clone;
		try {
			clone = (XEventImpl)super.clone();
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
			return null;
		}
		clone.attributes = (XAttributeMap)attributes.clone();
		return clone;
	}

}
