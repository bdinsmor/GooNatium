
import { Request, Response } from "express";
import { check, validationResult } from "express-validator";
import { Client } from "@elastic/elasticsearch";

const client = new Client({  
   cloud: {
    id: 'name:GooNatium:dXMtZWFzdC0xLmF3cy5mb3VuZC5pbyQwNTM1MzAxZjFhMTY0NDYyYTk1YWI1ODYxOWY2ZmRkOCQ0ZmJmZGVkMzY2M2M0NDFhOTVjMzBmY2Q5MzkxMzA0Zg==',
  }, auth: {
       username: "elastic",
       password: "s9jBhrfgIavPvIHYSG7fUPyb "
   }
    
});

/**
 * GET /getApi
 * Contact form page.
 */
export const getApi = (req: Request, res: Response) => {
    res.send(client.cluster.health({}));
};

/**
 * POST /contact
 * Send a contact form via Nodemailer.
 */
export const postGeoJSON = async (req: Request, res: Response) => {
    await check("data", "Name cannot be blank").not().isEmpty().run(req);
    await check("email", "Email is not valid").isEmail().run(req);
    await check("message", "Message cannot be blank").not().isEmpty().run(req);

    const errors = validationResult(req);

    if (!errors.isEmpty()) {
        req.flash("errors", errors.array());
        return res.redirect("/contact");
    }

};
