N::Person {
    name: String
}

N::Company {
    name: String
}

E::WorksAt {
    From: Person,
    To: Company,
    Properties: {
        INDEX since: String
    }
}
