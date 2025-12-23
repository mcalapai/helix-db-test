N::User {
    INDEX name: String,
}

QUERY create_user(name: String) =>
    user <- AddN<User>({name: name})
    RETURN user
