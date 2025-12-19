QUERY add_and_lookup(since: String) =>
    person <- AddN<Person>({name: "Ada"})
    company <- AddN<Company>({name: "Helix"})
    edge <- AddE<WorksAt>({since: since})::From(person)::To(company)
    found <- E<WorksAt>({since: since})
    RETURN edge, found

QUERY update_since(old: String, new: String) =>
    updated <- E<WorksAt>({since: old})::UPDATE({since: new})
    lookup <- E<WorksAt>({since: new})
    RETURN updated, lookup
