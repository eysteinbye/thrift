/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import Foundation

public struct TSet<TElement : TSerializable & Hashable> : SetAlgebra, Hashable, Collection, ExpressibleByArrayLiteral, TSerializable {
    
  /// Typealias for Storage type
  public typealias Storage = Set<TElement>
  
  
  /// Internal Storage used for TSet (Set\<TElement\>)
  internal var storage : Storage
  
  
  /// Mark: Collection
  
  public typealias TIndices = Storage.Indices
  public typealias TIndex = Storage.Index
  public typealias TIndexDistance = Storage.IndexDistance
  public typealias TSubSequence = Storage.SubSequence
  
  
  public var indices: TIndices { return storage.indices }
  
  // Must implement isEmpty even though both SetAlgebra and Collection provide it due to their conflciting default implementations
  public var isEmpty: Bool { return storage.isEmpty }
  
  public func distance(from start: TIndex, to end: TIndex) -> TIndexDistance {
    return storage.distance(from: start, to: end)
  }
  
  public func index(_ i: TIndex, offsetBy n: TIndexDistance) -> TIndex {
    return storage.index(i, offsetBy: n)
  }
  
  public func index(_ i: TIndex, offsetBy n: TIndexDistance, limitedBy limit: TIndex) -> TIndex? {
    return storage.index(i, offsetBy: n, limitedBy: limit)
  }
  
  public subscript (position: Set<TElement>.Index) -> TElement {
    return storage[position]
  }
  
  /// Mark: SetAlgebra
  internal init(storage: Set<TElement>) {
    self.storage = storage
  }
  
  public func contains(_ member: TElement) -> Bool {
    return storage.contains(member)
  }
  
  public mutating func insert(_ newMember: TElement) -> (inserted: Bool, memberAfterInsert: TElement) {
    return storage.insert(newMember)
  }
  
  public mutating func remove(_ member: TElement) -> TElement? {
    return storage.remove(member)
  }
  
  public func union(_ other: TSet<TElement>) -> TSet {
    return TSet(storage: storage.union(other.storage))
  }
  
  public mutating func formIntersection(_ other: TSet<TElement>) {
    return storage.formIntersection(other.storage)
  }
  
  public mutating func formSymmetricDifference(_ other: TSet<TElement>) {
    return storage.formSymmetricDifference(other.storage)
  }
  
  public mutating func formUnion(_ other: TSet<TElement>) {
    return storage.formUnion(other.storage)
  }
  
  public func intersection(_ other: TSet<TElement>) -> TSet {
    return TSet(storage: storage.intersection(other.storage))
  }
  
  public func symmetricDifference(_ other: TSet<TElement>) -> TSet {
    return TSet(storage: storage.symmetricDifference(other.storage))
  }
  
  public mutating func update(with newMember: TElement) -> TElement? {
    return storage.update(with: newMember)
  }
  
  /// Mark: IndexableBase
  
  public var startIndex: TIndex { return storage.startIndex }
  public var endIndex: TIndex { return storage.endIndex }
  public func index(after i: TIndex) -> TIndex {
    return storage.index(after: i)
  }

  public func formIndex(after i: inout Storage.Index) {
    storage.formIndex(after: &i)
  }
  
  public subscript(bounds: Range<TIndex>) -> TSubSequence {
    return storage[bounds]
  }

  
  /// Mark: Hashable
  public var hashValue : Int {
    let prime = 31
    var result = 1
    for element in storage {
      result = prime &* result &+ element.hashValue
    }
    return result
  }
  
  /// Mark: TSerializable
  public static var thriftType : TType { return .set }
  
  public init() {
    storage = Storage()
  }
  
  public init(arrayLiteral elements: TElement...) {
    self.storage = Storage(elements)
  }
  
  public init<Source : Sequence>(_ sequence: Source) where Source.Iterator.Element == TElement {
    storage = Storage(sequence)
  }
  
  public static func read(from proto: TProtocol) throws -> TSet {
    let (elementType, size) = try proto.readSetBegin()
    if elementType != TElement.thriftType {
      throw TProtocolError(error: .invalidData,
                           extendedError: .unexpectedType(type: elementType))
    }
    var set = TSet()
    for _ in 0..<size {
      let element = try TElement.read(from: proto)
      set.storage.insert(element)
    }
    try proto.readSetEnd()
    return set
  }
  
  public func write(to proto: TProtocol) throws {
    try proto.writeSetBegin(elementType: TElement.thriftType, size: Int32(self.count))
    for element in self.storage {
      try TElement.write(element, to: proto)
    }
    try proto.writeSetEnd()
  }
}

extension TSet: CustomStringConvertible, CustomDebugStringConvertible {
  public var description : String {
    return storage.description
  }
  public var debugDescription : String {
    return storage.debugDescription
  }
  
}

public func ==<TElement>(lhs: TSet<TElement>, rhs: TSet<TElement>) -> Bool {
  return lhs.storage == rhs.storage
}

